use quote::quote_spanned;
use slotmap::Key;

use super::{
    OperatorCategory, OperatorConstraints, OperatorWriteOutput, WriteContextArgs, RANGE_0, RANGE_1,
};

/// Internal pass-through used after Hydro network serialization to count exact
/// messages and serialized payload bytes in the containing subgraph.
pub const _NETWORK_METRICS: OperatorConstraints = OperatorConstraints {
    name: "_network_metrics",
    categories: &[OperatorCategory::Map],
    hard_range_inn: RANGE_1,
    soft_range_inn: RANGE_1,
    hard_range_out: RANGE_1,
    soft_range_out: RANGE_1,
    num_args: 0,
    persistence_args: RANGE_0,
    type_args: RANGE_0,
    is_external_input: false,
    flo_type: None,
    ports_inn: None,
    ports_out: None,
    input_delaytype_fn: |_| None,
    write_fn: |&WriteContextArgs {
                   root,
                   op_span,
                   ident,
                   inputs,
                   outputs,
                   is_pull,
                   subgraph_id,
                   ..
               },
               _| {
        let sg_ffi = subgraph_id.data().as_ffi();
        let input = &inputs[0];
        let output = &outputs[0];
        let inspect = quote_spanned! {op_span=>
            |item| {
                let metrics = &context.metrics().subgraphs[
                    #root::slotmap::KeyData::from_ffi(#sg_ffi).into()
                ];
                metrics.network_message_count.update(|count| count + 1);
                metrics.network_byte_count.update(|count| {
                    count + #root::scheduled::metrics::SerializedPayload::serialized_payload_len(item)
                });
            }
        };
        let write_iterator = if is_pull {
            quote_spanned! {op_span=>
                let #ident = #root::dfir_pipes::pull::Pull::inspect(#input, #inspect);
            }
        } else {
            quote_spanned! {op_span=>
                let #ident = #root::dfir_pipes::push::inspect(#inspect, #output);
            }
        };
        Ok(OperatorWriteOutput {
            write_iterator,
            ..Default::default()
        })
    },
};
