//! Agent-fleet compute backend: agents dial the coordinator over mTLS gRPC
//! (`orbd.control.v1.AgentControl`) and run orbs with their local runtimes.
//! The gRPC protocol is this backend's private transport.
