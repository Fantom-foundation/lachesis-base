package hash

import (
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const (
	// HashLength is the expected length of the hash
	HashLength = 32
)

var (
	// Zero is an empty hash.
	Zero  = Hash{}
	hashT = reflect.TypeOf(Hash{})
)

// Hash represents the 32 byte hash of arbitrary data.
type Hash [HashLength]byte

type Hashes []Hash

type HashesSet map[Hash]struct{}

// BytesToHash sets b to hash.
// If b is larger than len(h), b will be cropped from the left.
func BytesToHash(b []byte) Hash {
	var h Hash
	h.SetBytes(b)
	return h
}

// BigToHash sets byte representation of b to hash.
// If b is larger than len(h), b will be cropped from the left.
func BigToHash(b *big.Int) Hash { return BytesToHash(b.Bytes()) }

// HexToHash sets byte representation of s to hash.
// If b is larger than len(h), b will be cropped from the left.
func HexToHash(s string) Hash { return BytesToHash(hexutil.MustDecode(s)) }

// Bytes gets the byte representation of the underlying hash.
func (h Hash) Bytes() []byte { return h[:] }

// Big converts a hash to a big integer.
func (h Hash) Big() *big.Int { return new(big.Int).SetBytes(h[:]) }

// Hex converts a hash to a hex string.
func (h Hash) Hex() string { return hexutil.Encode(h[:]) }

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (h Hash) TerminalString() string {
	return fmt.Sprintf("%x…%x", h[:3], h[29:])
}

// String implements the stringer interface and is used also by the logger when
// doing full logging into a file.
func (h Hash) String() string {
	return h.Hex()
}

// Format implements fmt.Formatter, forcing the byte slice to be formatted as is,
// without going through the stringer interface used for logging.
func (h Hash) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, "%"+string(c), h[:])
}

// UnmarshalText parses a hash in hex syntax.
func (h *Hash) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("Hash", input, h[:])
}

// UnmarshalJSON parses a hash in hex syntax.
func (h *Hash) UnmarshalJSON(input []byte) error {
	return hexutil.UnmarshalFixedJSON(hashT, input, h[:])
}

// MarshalText returns the hex representation of h.
func (h Hash) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

// setBytes sets the hash to the value of b.
// If b is larger than len(h), b will be cropped from the left.
func (h *Hash) SetBytes(b []byte) {
	if len(b) > len(h) {
		b = b[len(b)-HashLength:]
	}

	copy(h[HashLength-len(b):], b)
}

// FakeHash generates random fake hash for testing purpose.
func FakeHash(seed ...int64) (h common.Hash) {
	randRead := rand.Read

	if len(seed) > 0 {
		src := rand.NewSource(seed[0])
		rnd := rand.New(src)
		randRead = rnd.Read
	}

	_, err := randRead(h[:])
	if err != nil {
		panic(err)
	}
	return
}

/*
 * HashesSet methods:
 */

// NewHashesSet makes hash index.
func NewHashesSet(h ...Hash) HashesSet {
	hh := HashesSet{}
	hh.Add(h...)
	return hh
}

// Copy copies hashes to a new structure.
func (hh HashesSet) Copy() HashesSet {
	ee := make(HashesSet, len(hh))
	for k, v := range hh {
		ee[k] = v
	}

	return ee
}

// String returns human readable string representation.
func (hh HashesSet) String() string {
	ss := make([]string, 0, len(hh))
	for h := range hh {
		ss = append(ss, h.String())
	}
	return "[" + strings.Join(ss, ", ") + "]"
}

// Slice returns whole index as slice.
func (hh HashesSet) Slice() Hashes {
	arr := make(Hashes, len(hh))
	i := 0
	for h := range hh {
		arr[i] = h
		i++
	}
	return arr
}

// Add appends hash to the index.
func (hh HashesSet) Add(hash ...Hash) {
	for _, h := range hash {
		hh[h] = struct{}{}
	}
	return
}

// Erase erase hash from the index.
func (hh HashesSet) Erase(hash ...Hash) {
	for _, h := range hash {
		delete(hh, h)
	}
	return
}

// Contains returns true if hash is in.
func (hh HashesSet) Contains(hash Hash) bool {
	_, ok := hh[hash]
	return ok
}

/*
 * Hashes methods:
 */

// NewHashes makes hash slice.
func NewHashes(h ...Hash) Hashes {
	hh := Hashes{}
	hh.Add(h...)
	return hh
}

// Copy copies hashes to a new structure.
func (hh Hashes) Copy() Hashes {
	ee := make(Hashes, len(hh))
	for k, v := range hh {
		ee[k] = v
	}

	return ee
}

// String returns human readable string representation.
func (hh Hashes) String() string {
	ss := make([]string, 0, len(hh))
	for _, h := range hh {
		ss = append(ss, h.String())
	}
	return "[" + strings.Join(ss, ", ") + "]"
}

// Set returns whole index as a HashesSet.
func (hh Hashes) Set() HashesSet {
	set := make(HashesSet, len(hh))
	for _, h := range hh {
		set[h] = struct{}{}
	}
	return set
}

// Add appends hash to the slice.
func (hh *Hashes) Add(hash ...Hash) {
	*hh = append(*hh, hash...)
}
