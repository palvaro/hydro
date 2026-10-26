use std::rc::Rc;

use hydro_lang::compile::ir::HydroIrOpMetadata;
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::networking::{NetworkingInfo, TcpFault};
use hydro_lang::sim::network_hook::{ChannelInfo, LocationInfo};

use super::*;

/// A process `Source` and a three-member cluster `Replica`.
fn locations() -> Vec<LocationInfo> {
    vec![
        LocationInfo {
            id: LocationId::Process("loc1v1".parse::<LocationKey>().unwrap()),
            name: "test::Source".to_owned(),
            members: None,
        },
        LocationInfo {
            id: LocationId::Cluster("loc2v1".parse::<LocationKey>().unwrap()),
            name: "test::Replica".to_owned(),
            members: Some(vec![0, 1, 2]),
        },
    ]
}

fn channel(id: usize, name: Option<&str>) -> ChannelInfo {
    let l = locations();
    ChannelInfo {
        id,
        name: name.map(str::to_owned),
        from: l[0].id.clone(),
        to: l[1].id.clone(),
        networking: NetworkingInfo::Tcp {
            fault: TcpFault::LossyDelayedForever,
        },
        intercepted: true,
        op: HydroIrOpMetadata::new(),
    }
}

fn message(names: &NodeNames, channel: usize, from: &str, to: &str, bytes: &[u8]) -> MessageId {
    MessageId {
        channel,
        from: names.id(from).unwrap().clone(),
        to: names.id(to).unwrap().clone(),
        bytes: Rc::from(bytes),
        copy: 0,
    }
}

fn fact(bytes: &[u8], debug: &str) -> Fact {
    let mut f = Fact::from_bytes(bytes.to_vec());
    f.debug = Some(debug.to_owned());
    f
}

#[test]
fn payloads_decode_through_facts_with_the_same_bytes() {
    let nodes = NodeNames::new(&locations());
    let names = ReportNames::new(&nodes, &[channel(3, Some("bcast"))])
        .with_facts([&fact(&[7, 0, 0, 0], "Item(7)")]);
    let m = message(&nodes, 3, "Source", "Replica@1", &[7, 0, 0, 0]);
    assert_eq!(names.message(&m), "bcast[Source→Replica@1]: Item(7)");
    // No fact with these bytes: short hex, never raw bincode of unknown length.
    let other = message(
        &nodes,
        3,
        "Source",
        "Replica@1",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    assert_eq!(
        names.message(&other),
        "bcast[Source→Replica@1]: 0102030405060708…"
    );
}

#[test]
fn unnamed_channels_fall_back_to_their_site_then_their_id() {
    let nodes = NodeNames::new(&locations());
    // Named after the site that built it (here: this test's `channel` helper, the function
    // whose tail expression built the metadata, or its `file:line`).
    let names = ReportNames::new(&nodes, &[channel(4, None)]);
    let name = names.channel(4);
    assert!(!name.is_empty() && name != "c4", "{name}");
    // A channel outside the table.
    assert_eq!(names.channel(9), "c9");
}

#[test]
fn faults_name_nodes_and_messages() {
    let nodes = NodeNames::new(&locations());
    let names =
        ReportNames::new(&nodes, &[channel(3, Some("bcast"))]).with_facts([&fact(&[7], "Item(7)")]);
    let m = message(&nodes, 3, "Source", "Replica@2", &[7]);
    let mut set = FaultSet::new();
    set.insert(Fault::Omit(m.clone()));
    set.insert(Fault::crash_after("Source", Some(m)));
    set.insert(Fault::crash_after("Replica@0", None));
    assert_eq!(
        names.fault_set(&set),
        "{crash(Replica@0), crash(Source after bcast[Source→Replica@2]: Item(7)), \
         omit(bcast[Source→Replica@2]: Item(7))}"
    );
}

#[test]
fn long_payloads_go_to_a_legend_once() {
    let nodes = NodeNames::new(&locations());
    let long = "Write { key: \"some key\", value: 123456789, ballot: 3 }";
    let names = ReportNames::new(&nodes, &[channel(3, Some("w"))]).with_facts([&fact(&[1], long)]);
    let a = message(&nodes, 3, "Source", "Replica@0", &[1]);
    let b = message(&nodes, 3, "Source", "Replica@1", &[1]);
    assert_eq!(names.message(&a), "w[Source→Replica@0]: ‹1›");
    assert_eq!(names.message(&b), "w[Source→Replica@1]: ‹1›");
    assert_eq!(names.abbreviate("short".to_owned()), "short");
    assert_eq!(names.legend(), vec![(1, long.to_owned())]);
}

#[test]
fn faults_in_words() {
    let nodes = NodeNames::new(&locations());
    let names =
        ReportNames::new(&nodes, &[channel(3, Some("bcast"))]).with_facts([&fact(&[7], "Item(7)")]);
    let m = message(&nodes, 3, "Source", "Replica@2", &[7]);
    let words = |faults: Vec<Fault<MessageId>>| {
        let mut set = FaultSet::new();
        for f in faults {
            set.insert(f);
        }
        names.fault_set_in_words(&set)
    };
    assert_eq!(words(vec![]), None);
    assert_eq!(
        words(vec![Fault::crash_after("Source", Some(m.clone()))]),
        Some((
            "a single crash of Source, right after it sends Item(7) to Replica@2 (on bcast),"
                .to_owned(),
            false
        ))
    );
    assert_eq!(
        words(vec![Fault::Omit(m.clone())]),
        Some((
            "losing the message Item(7) from Source to Replica@2 (on bcast)".to_owned(),
            false
        ))
    );
    let mut again = m;
    again.copy = 1;
    assert_eq!(
        words(vec![
            Fault::Omit(again),
            Fault::crash_after("Replica@0", None)
        ]),
        Some((
            "a crash of Replica@0 before it sends anything and the loss of the 2nd copy of the \
             message Item(7) from Source to Replica@2 (on bcast)"
                .to_owned(),
            true
        ))
    );
    assert_eq!(
        [1, 2, 3, 4, 11, 12, 13, 21, 22, 101].map(ordinal),
        [
            "1st", "2nd", "3rd", "4th", "11th", "12th", "13th", "21st", "22nd", "101st"
        ]
    );
}
