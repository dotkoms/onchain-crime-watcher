//! Original-bag accounting. Observed proceeds never stand in for tokens disposed.
//! This module is pure; live capture/funding inputs must independently establish coverage.
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bag {
    original: Option<u64>,
    original_remaining: Option<u64>,
    inventory: Option<u64>,
    original_cost_lamports: u64,
    proceeds_lamports: u128,
    complete: bool,
    allocation_known: bool,
}
impl Bag {
    pub fn open(
        first_tokens: u64,
        post_balance: Option<u64>,
        first_cost_lamports: u64,
    ) -> Option<Self> {
        if first_tokens == 0
            || first_cost_lamports == 0
            || post_balance.is_some_and(|q| q < first_tokens)
        {
            return None;
        }
        Some(Self {
            original: post_balance,
            original_remaining: post_balance,
            inventory: post_balance,
            original_cost_lamports: first_cost_lamports,
            proceeds_lamports: 0,
            complete: post_balance.is_some(),
            allocation_known: post_balance.is_some(),
        })
    }
    pub fn validate(&self) -> bool {
        self.original_cost_lamports > 0
            && self.original.is_none_or(|q| q > 0)
            && match (self.original, self.original_remaining) {
                (Some(q), Some(left)) => left <= q,
                (None, None) => true,
                _ => false,
            }
            && (!self.allocation_known || self.original.is_some() && self.inventory.is_some())
            && (!self.complete
                || self.allocation_known && self.original_remaining <= self.inventory)
    }
    pub fn gap(&mut self) {
        self.complete = false;
        self.allocation_known = false;
    }
    pub fn buy(&mut self, tokens: u64, post: Option<u64>) {
        let expected = self.inventory.and_then(|n| n.checked_add(tokens));
        if tokens == 0
            || expected.is_none()
            || post.is_none()
            || post.is_some_and(|n| Some(n) != expected)
        {
            self.gap();
        }
        self.inventory = post.or(expected);
    }
    pub fn sell(&mut self, tokens: u64, post: Option<u64>, proceeds_lamports: u64) {
        let before = self.inventory;
        let expected = before.and_then(|n| n.checked_sub(tokens));
        if tokens == 0
            || expected.is_none()
            || post.is_none()
            || post.is_some_and(|n| Some(n) != expected)
        {
            self.gap();
        }
        if self.allocation_known
            && let (Some(before), Some(left)) = (before, self.original_remaining)
            && before > 0
        {
            let allocated = if tokens == before {
                left
            } else {
                ((left as u128 * tokens as u128) / before as u128) as u64
            };
            self.original_remaining = left.checked_sub(allocated);
        }
        self.inventory = post.or(expected);
        if let Some(total) = self
            .proceeds_lamports
            .checked_add(proceeds_lamports as u128)
        {
            self.proceeds_lamports = total;
        } else {
            self.gap();
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Tier {
    None,
    Near,
    Fire,
    Cleared,
}
#[derive(Debug, Serialize)]
pub struct Progress {
    pub tier: Tier,
    /// Certified lower display bound, never sale proceeds / purchase spend.
    pub sold_bps_lower: u16,
    pub proceeds_lamports: u128,
    pub original_cost_lamports: u128,
    pub opening_tokens: u128,
    pub current_tokens: Option<u128>,
    pub partial: bool,
    pub minimums_met: bool,
    pub threshold_uncertain: bool,
    pub price_context_available: bool,
}
fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}
/// Compare a sum of weighted exact fractions to a basis-point threshold. Integer
/// interval bounds decide ordinary cases exactly. Only an unresolved boundary needs
/// rational addition; checked overflow returns unknown, never a guessed tier.
fn reaches(parts: &[(u64, u64, u64)], cost: u128, bps: u128) -> Option<bool> {
    let mut lower = 0u128;
    let mut rest = Vec::new();
    for &(weight, sold, opening) in parts {
        let numerator = weight as u128 * sold as u128;
        lower = lower.checked_add(numerator / opening as u128)?;
        let r = numerator % opening as u128;
        if r > 0 {
            rest.push((r, opening as u128));
        }
    }
    let target = cost.checked_mul(bps)?;
    let lower_scaled = lower.checked_mul(10_000)?;
    if lower_scaled >= target {
        return Some(true);
    }
    if (lower + rest.len() as u128).checked_mul(10_000)? <= target {
        return Some(false);
    }
    let (mut n, mut d) = (0u128, 1u128);
    for (r, denom) in rest {
        let g = gcd(d, denom);
        n = n
            .checked_mul(denom / g)?
            .checked_add(r.checked_mul(d / g)?)?;
        d = d.checked_mul(denom / g)?;
        let g = gcd(n, d);
        n /= g;
        d /= g;
    }
    Some(compare_ratio(n, d, target - lower_scaled, 10_000) != std::cmp::Ordering::Less)
}
/// Continued fractions compare without overflowing cross-products.
fn compare_ratio(mut a: u128, mut b: u128, mut c: u128, mut d: u128) -> std::cmp::Ordering {
    let mut reverse = false;
    loop {
        let order = (a / b).cmp(&(c / d));
        if order != std::cmp::Ordering::Equal {
            return if reverse { order.reverse() } else { order };
        }
        let (r, s) = (a % b, c % d);
        if r == 0 || s == 0 {
            let order = r.cmp(&s);
            return if reverse { order.reverse() } else { order };
        }
        (a, b, c, d) = (b, r, d, s);
        reverse = !reverse;
    }
}
/// Evaluate one funding cluster; unrelated roots must never be pooled to meet minimums.
/// Price-context availability must be established by the caller from actual observations.
pub fn progress(
    bags: &[Bag],
    supply: Option<u64>,
    price_context: bool,
    window_complete: bool,
) -> Option<Progress> {
    let supply = supply.filter(|s| *s > 0);
    if bags.is_empty() || bags.len() > 256 || bags.iter().any(|b| !b.validate()) {
        return None;
    }
    let mut parts = Vec::new();
    let (mut cost, mut opening, mut lower, mut proceeds) = (0u128, 0u128, 0u128, 0u128);
    let mut current = Some(0u128);
    for b in bags {
        let (q, left) = (b.original?, b.original_remaining?);
        cost = cost.checked_add(b.original_cost_lamports as u128)?;
        opening = opening.checked_add(q as u128)?;
        proceeds = proceeds.checked_add(b.proceeds_lamports)?;
        let sold = q.checked_sub(left)?;
        lower = lower.checked_add(b.original_cost_lamports as u128 * sold as u128 / q as u128)?;
        parts.push((b.original_cost_lamports, sold, q));
        current = current.and_then(|n| b.inventory.and_then(|q| n.checked_add(q as u128)));
    }
    let minimums = bags.len() >= 5
        && cost >= 10_000_000_000
        && supply.is_some_and(|s| opening <= s as u128 && opening * 100 >= s as u128 * 2);
    let partial = !window_complete
        || bags.iter().any(|b| !b.complete)
        || supply.is_none_or(|s| opening > s as u128);
    let mut tier = Tier::None;
    let mut uncertain = false;
    if minimums {
        for (threshold, next) in [(6000, Tier::Near), (8000, Tier::Fire)] {
            match reaches(&parts, cost, threshold) {
                Some(true) => tier = next,
                Some(false) => {}
                None => uncertain = true,
            }
        }
        if !partial && price_context && current.is_some_and(|q| q * 100 <= opening * 5) {
            match reaches(&parts, cost, 9500) {
                Some(true) => tier = Tier::Cleared,
                Some(false) => {}
                None => uncertain = true,
            }
        }
    }
    Some(Progress {
        tier,
        sold_bps_lower: ((lower * 10_000) / cost) as u16,
        proceeds_lamports: proceeds,
        original_cost_lamports: cost,
        opening_tokens: opening,
        current_tokens: current,
        partial,
        minimums_met: minimums,
        threshold_uncertain: uncertain,
        price_context_available: price_context,
    })
}
pub mod capture;
pub mod funding;

#[cfg(test)]
mod tests;
