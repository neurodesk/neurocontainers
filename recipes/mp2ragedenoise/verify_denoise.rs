use mp2rage_cli::{load::load, nifti_io::write_nifti_f32};
use mp2rage_core::denoise::robust_combination;
fn main() {
    let p = std::env::args().nth(1).expect("example directory with trailing slash");
    let u = load(&format!("{p}MP2RAGE_UNI.nii.gz")).unwrap();
    let a = load(&format!("{p}MP2RAGE_INV1.nii.gz")).unwrap();
    let b = load(&format!("{p}MP2RAGE_INV2.nii.gz")).unwrap();
    let r = robust_combination(&u.c0, &a.c0, &b.c0, 6.0);
    write_nifti_f32(&format!("{p}webapp-reference.nii.gz"), &r.mapv(|v| v as f32), &u.affine).unwrap();
}
