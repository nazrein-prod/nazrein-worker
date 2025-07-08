package utils

import "hash/fnv"

func HashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// Convert MD5 hex string to uint64 for database storage
// GOOGLE: Why don't we store the MD5 hash as a string in the DB?
func MD5ToUint64(md5Hash string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(md5Hash))
	return h.Sum64()
}
