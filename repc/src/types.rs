use std::{
    num::NonZeroU64,
    ops::{Add, AddAssign},
};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Term(NonZeroU64);

impl Term {
    pub fn new(term: u64) -> Self {
        Self(NonZeroU64::new(term).expect("term should be greater than 0"))
    }

    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

impl Default for Term {
    fn default() -> Self {
        Term::new(1)
    }
}

impl Add<u64> for Term {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Term::new(self.0.get() + rhs)
    }
}

impl AddAssign<u64> for Term {
    fn add_assign(&mut self, rhs: u64) {
        *self = Term::new(self.0.get() + rhs)
    }
}
