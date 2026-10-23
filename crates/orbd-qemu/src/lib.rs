//! Running QEMU/KVM VMs whose full state can be saved to a file and loaded again:
//!
//! * [`qmp`]: a narrow, typed QMP client.
//! * [`probe`]: QEMU version, machine type and host capability checks.
//! * [`launch`]: the guest-visible machine definition, host plumbing, and the
//!   supervised start of QEMU (on [`vm::process`]).
//! * [`migration`]: saving to and loading from a file with `mapped-ram` file migration.
//!
//! [`vm`] re-exports the VMM-neutral pieces (durable files, process supervision,
//! checkpoint directories) these build on.

pub mod launch;
pub mod migration;
pub mod probe;
pub mod qmp;

pub use orbd_vm as vm;
