fn main() {
    println!("cargo::rustc-check-cfg=cfg(stageleft_macro)");
    stageleft_tool::gen_final!();
}
