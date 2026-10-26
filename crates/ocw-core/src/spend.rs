use crate::storage::{atomic_write, new_private};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u8,
    day: u64,
    reserved: u64,
    checksum: String,
}

impl State {
    fn new(day: u64, reserved: u64) -> Self {
        let checksum = format!("{:x}", Sha256::digest(format!("1:{day}:{reserved}")));
        Self {
            version: 1,
            day,
            reserved,
            checksum,
        }
    }
}
pub struct Ledger {
    path: PathBuf,
    state: State,
    limit: u64,
    block: u64,
}
impl Ledger {
    pub fn initialize(path: &Path, day: u64) -> io::Result<()> {
        let bytes = serde_json::to_vec(&State::new(day, 0))?;
        let mut f = new_private(path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        fs::File::open(
            path.parent()
                .ok_or_else(|| io::Error::other("missing parent"))?,
        )?
        .sync_all()
    }
    pub fn open(path: &Path, _day: u64, daily: u64, margin: u64, block: u64) -> io::Result<Self> {
        if margin >= daily || block == 0 || block > daily - margin {
            return Err(io::Error::other("invalid spend policy"));
        }
        if path.is_symlink() || fs::metadata(path)?.len() > 4096 {
            return Err(io::Error::other("invalid spend ledger"));
        }
        let state: State = serde_json::from_slice(&fs::read(path)?)
            .map_err(|_| io::Error::other("corrupt spend ledger"))?;
        if state.version != 1 || state.checksum != State::new(state.day, state.reserved).checksum {
            return Err(io::Error::other("corrupt spend ledger"));
        }
        Ok(Self {
            path: path.into(),
            state,
            limit: daily - margin,
            block,
        })
    }
    pub fn reserved(&self) -> u64 {
        self.state.reserved
    }
    pub fn reserve(&mut self, day: u64) -> io::Result<u64> {
        let (next_day, current) = if day > self.state.day {
            (day, 0)
        } else {
            (self.state.day, self.state.reserved)
        };
        let grant = self.block.min(self.limit.saturating_sub(current));
        if grant == 0 {
            return Ok(0);
        }
        let state = State::new(next_day, current + grant);
        atomic_write(&self.path, &serde_json::to_vec(&state)?)?;
        self.state = state;
        Ok(grant)
    }
}
