`thirdparty` contains some 3rd party code which is not suitable for the standard Go vendoring

- github.com/google/protobuf is some supporting `.proto` definitions that we use when generating the protocol buffers types. The `.h`, `.cc` and some other unneeded files have been stripped out.
