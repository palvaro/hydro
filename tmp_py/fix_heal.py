p='hydrofrac/src/check/engine.rs'; s=open(p).read()
o="""        let flow = flow
            .skip_consistency_assertions()"""
assert s.count(o)==1
s=s.replace(o,"""        let flow = flow
            .refuse_sim_faults("hydrofrac")
            .skip_consistency_assertions()""")
open(p,'w').write(s)
p='hydrofrac/src/fault/mod.rs'; s=open(p).read()
o="""    fn record(self) -> InstanceRecord {
        InstanceRecord {"""
assert s.count(o)==1
s=s.replace(o,"""    fn record(self) -> InstanceRecord {
        EventLog::check_heal(&self.log);
        InstanceRecord {""")
o="""    /// The sends, in program order, as `(id, message, ordinal, fate)`."""
assert s.count(o)==1
s=s.replace(o,"""    /// Panics unless the heal ended the faults: after an [`Event::Heal`], no send is omitted
    /// and no crash fires. Lineage relies on it: it marks a delivery omittable only if it was
    /// sent before the heal, and keeps every delivery's sender-crash dependency, since a node
    /// that crashed before the heal sends nothing after it. Sends after the heal are fine.
    pub fn check_heal(events: &[Event]) {
        let Some(heal) = events.iter().position(|e| matches!(e, Event::Heal)) else {
            return;
        };
        for e in &events[heal..] {
            match e {
                Event::Sent {
                    fate: Fate::Omitted,
                    msg,
                    ..
                } => panic!("hydrofrac: `{msg}` was omitted after the network healed"),
                Event::Halted { node } => {
                    panic!("hydrofrac: a crash of `{node}` fired after the network healed")
                }
                _ => {}
            }
        }
    }

    /// The sends, in program order, as `(id, message, ordinal, fate)`.""")
open(p,'w').write(s)
print("ok")
