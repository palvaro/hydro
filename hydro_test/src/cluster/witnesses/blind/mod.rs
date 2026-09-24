//! Blind witness programs written from the self-contained task brief.
//!
//! Each row reports the label basis and the headline assertion measured by that program's own
//! simulator harness. Hazardous labels use a deterministic bounded trigger followed by a distant
//! tail. Benign labels use a closed-form assurance argument checked by deterministic and fuzzed
//! schedules.
//!
//! | program | configuration | label | basis | headline number |
//! |---|---|---|---|---|
//! | [`batch_flush`] | size 8, flush after 4 ticks, sink capacity 8 | hazardous | exhibited collapse | tail: 18 completions and 225,506 copies; sink FIFO 240,682 -> 463,658 |
//! | [`bounded_fanout`] | fan-out 3 | benign | assurance argument: `wire = processed = 3P` | burst run: 2,140 inputs, 6,420 wire messages, 6,420 processed |
//! | [`credit_flow`] | capacities 4, credit window 16 | benign | assurance argument: four records per input | burst run: 520 inputs and exactly 2,080 work records |
//! | [`log_catchup`] | chunked fetch, retry after 4 ticks | hazardous | exhibited collapse | tail: 1 completion, 192 retries, backlog 1,624 -> 1,940 |
//! | [`token_bucket`] | refill 5, bucket capacity 10 | benign | assurance argument: one send and one serve per request | burst run: exactly 1,200 sends and 1,200 serves |
//! | [`two_phase_commit`] | participant capacity 5, prepare timeout 4 | hazardous | exhibited collapse | tail: 8 completions, 56,742 retries, backlog 73,335 -> 129,260 |
//! | [`visibility_queue`] | worker capacity 5, visibility timeout 6 | hazardous | exhibited collapse | tail: 63 first and 937 repeat processings; worker backlog 50,216 -> 85,465 |

pub mod batch_flush;
pub mod bounded_fanout;
pub mod credit_flow;
pub mod log_catchup;
pub mod token_bucket;
pub mod two_phase_commit;
pub mod visibility_queue;
