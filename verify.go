package rsmq

import "strings"

const defNamespace = "rsmq"
const namespaceSeparator = ":"

func verifyNamespace(ns *string) {
	if *ns == "" {
		*ns = defNamespace + namespaceSeparator
	} else if !strings.HasSuffix(*ns, namespaceSeparator) {
		*ns += namespaceSeparator
	}
}
