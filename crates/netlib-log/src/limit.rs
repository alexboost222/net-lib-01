//! Rate limiting of hot events.

use crate::{EventId, Level, LogSink, Property, Record, Template, Value, event_id};

const SLOTS: usize = 64;

#[derive(Clone, Copy, Default)]
struct Slot {
    id: EventId,
    window_start: u64,
    count: u32,
    suppressed: u32,
}

static SUPPRESSED: Template = Template {
    text: "Suppressed {Count} events {SuppressedEventId}",
    level: Level::Warn,
    id: event_id("Suppressed {Count} events {SuppressedEventId}"),
};

/// Wraps a sink and lets through at most `per_second` events of each [`EventId`] per
/// second; at the start of the next window it reports how many were suppressed.
pub struct RateLimited<S> {
    inner: S,
    per_second: u32,
    slots: [Slot; SLOTS],
}

impl<S: LogSink> RateLimited<S> {
    /// Wraps `inner`.
    pub fn new(inner: S, per_second: u32) -> Self {
        RateLimited {
            inner,
            per_second,
            slots: [Slot::default(); SLOTS],
        }
    }

    /// The wrapped sink.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// The wrapped sink.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S: LogSink> LogSink for RateLimited<S> {
    fn enabled(&self, level: Level) -> bool {
        self.inner.enabled(level)
    }

    fn log(&mut self, record: &Record<'_>) {
        let id = record.template.id;
        let slot = &mut self.slots[id as usize % SLOTS];
        if slot.id != id {
            *slot = Slot {
                id,
                window_start: record.time_us,
                ..Slot::default()
            };
        }
        if record.time_us.saturating_sub(slot.window_start) >= 1_000_000 {
            let suppressed = slot.suppressed;
            *slot = Slot {
                id,
                window_start: record.time_us,
                ..Slot::default()
            };
            if suppressed > 0 && self.inner.enabled(Level::Warn) {
                let props = [
                    Property {
                        name: "Count",
                        value: Value::U64(suppressed.into()),
                    },
                    Property {
                        name: "SuppressedEventId",
                        value: Value::U64(id.into()),
                    },
                ];
                self.inner.log(&Record {
                    template: &SUPPRESSED,
                    time_us: record.time_us,
                    properties: &props,
                });
            }
        }
        let slot = &mut self.slots[id as usize % SLOTS];
        if slot.count < self.per_second {
            slot.count += 1;
            self.inner.log(record);
        } else {
            slot.suppressed += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netlog;
    use alloc::vec::Vec;

    #[derive(Default)]
    struct Ids(Vec<(EventId, Option<u64>)>);

    impl LogSink for Ids {
        fn enabled(&self, _: Level) -> bool {
            true
        }
        fn log(&mut self, r: &Record<'_>) {
            let count = r.get("Count").map(|v| match v {
                Value::U64(c) => c,
                _ => 0,
            });
            self.0.push((r.template.id, count));
        }
    }

    #[test]
    fn l4_hot_events_are_limited_and_counted() {
        let mut s = RateLimited::new(Ids::default(), 10);
        for t in 0..1000u64 {
            netlog!(s, Debug, t * 1000, "Dropped datagram {Seq}", Seq = t);
        }
        assert_eq!(s.inner().0.len(), 10, "first second: 10 of 1000");
        netlog!(s, Debug, 1_500_000, "Dropped datagram {Seq}", Seq = 0u64);
        let last = &s.inner().0[s.inner().0.len() - 2..];
        assert_eq!(last[0], (SUPPRESSED.id, Some(990)));
    }
}
