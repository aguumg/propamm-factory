//! What every stock pair does, whoever issued the token: the pricer and its gates, the guard
//! on the token's transfer rules, and the US market hours. Each reads its design's answers
//! through the ports (`crate::ports`) and judges them the same way for every design.

pub mod guard;
pub mod hours;
pub mod pricer;
