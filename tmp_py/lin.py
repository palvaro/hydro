import re
p='tests.rs'
s=open(p).read()
s=s.replace('''    // The dead send is not in A's send sequence; the omitted one is (A did send it).
    let seq = &lineage.send_sequences()["A"];
    assert_eq!(seq.len(), 2);
    assert_eq!(seq[1].bytes[..], [2]);
}''','''}''')
s=s.replace('''    assert_eq!(d.len(), 2);
    assert_eq!(lineage.pre_heal_sends()["A"], 1);
    assert_eq!(lineage.send_sequences()["A"].len(), 2);

    // The crash right after A's first send fires before the heal and kills the post-heal send.
    let clause = lineage.clause_for(out);
    let first = msg(LOSSY, &node("A"), &node("C"), 1);
    let second = msg(LOSSY, &node("A"), &node("C"), 2);
    assert!(clause.hit_by(&Fault::crash_after("A", Some(first.clone()))));
    assert!(clause.hit_by(&Fault::Omit(first)));
    assert!(!clause.hit_by(&Fault::Omit(second.clone())));
    // A crash point named by a post-heal send never fires.
    assert!(!clause.hit_by(&Fault::crash_after("A", Some(second))));
}''','''    assert_eq!(d.len(), 2);

    // The crash right after A's first send fires before the heal and kills the post-heal send.
    // (No crash point names a post-heal send: the search's candidates come from pre-heal sends
    // only.)
    let clause = PairClause::robust(d.iter().cloned());
    let first = msg(LOSSY, &node("A"), &node("C"), 1);
    let second = msg(LOSSY, &node("A"), &node("C"), 2);
    assert!(clause.hit_by(&Fault::crash_after("A", Some(first.clone())), &()));
    assert!(clause.hit_by(&Fault::Omit(first), &()));
    assert!(!clause.hit_by(&Fault::Omit(second), &()));
}''')
s=s.replace('''/// The clause's kill relation, through the search's `hit_by`, on a two-send run.''','''/// The kill relation of a derivation, through the search's crash rule, on a two-send run.''')
s=s.replace('''    let clause = lineage.clause_for(out);
    assert!(clause.premises.is_empty());
''','''    let clause = PairClause::robust(lineage.derivation(out).iter().cloned());
''')
s=s.replace('''    let candidates: FaultSet<MessageId> = clause.candidate_faults().collect();
    assert_eq!(
        candidates,
        [Fault::Omit(m2), Fault::crash_after("A", Some(m1))].into()
    );
}''','''}''')
s=s.replace('''    assert!(lineage.clause_for(out).is_unkillable());''','''    assert!(lineage.derivation(out).is_empty());''')
# hit_by single arg -> add order
s=re.sub(r'clause\.hit_by\((&Fault::[^;]*?)\)\);', lambda m: 'clause.hit_by('+m.group(1)+', &()));', s)
open(p,'w').write(s)
p='sim_tests.rs'
s=open(p).read()
s=s.replace('lineage.clause_for(observations[0].event)','PairClause::robust(lineage.derivation(observations[0].event).iter().cloned())')
s=re.sub(r'clause\.hit_by\((&Fault::[^;]*?)\)\)', lambda m: 'clause.hit_by('+m.group(1)+', &()))', s)
open(p,'w').write(s)
