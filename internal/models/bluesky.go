package models

import (
	"time"
)

// FirehoseEvent is the wrapper we send down the channel
type FirehoseEvent struct {
	User string
	Post PostRecord
}

// PostRecord represents the "app.bsky.feed.post" lexicon.
type PostRecord struct {
	Uri       string      `json:"uri"`
	Type      string      `json:"$type" cbor:"$type"`
	Text      string      `json:"text" cbor:"text"`
	CreatedAt time.Time   `json:"createdAt" cbor:"createdAt"`
	Langs     []string    `json:"langs,omitempty" cbor:"langs,omitempty"`
	Reply     *ReplyRef   `json:"reply,omitempty" cbor:"reply,omitempty"`
	Facets    []Facet     `json:"facets,omitempty" cbor:"facets,omitempty"`
	Embed     *Embed      `json:"embed,omitempty" cbor:"embed,omitempty"`
	Tags      []string    `json:"tags,omitempty" cbor:"tags,omitempty"`
	Labels    *SelfLabels `json:"labels,omitempty" cbor:"labels,omitempty"`
	Subject   *StrongRef  `json:"subject,omitempty" cbor:"subject,omitempty"`
}

// ReplyRef points to the parent and the root of a thread.
type ReplyRef struct {
	Root   StrongRef `json:"root" cbor:"root"`
	Parent StrongRef `json:"parent" cbor:"parent"`
}

// StrongRef is a link to another record via URI and CID.
type StrongRef struct {
	Uri string `json:"uri" cbor:"uri"`
	Cid string `json:"cid" cbor:"cid"`
}

// Facet handles rich text like links and mentions.
type Facet struct {
	Index    FeaturesIndex    `json:"index" cbor:"index"`
	Features []map[string]any `json:"features" cbor:"features"`
}

type FeaturesIndex struct {
	ByteStart int `json:"byteStart" cbor:"byteStart"`
	ByteEnd   int `json:"byteEnd" cbor:"byteEnd"`
}

// Embed can be an image, an external link, a record (quote), etc.
type Embed struct {
	Type string `json:"$type" cbor:"$type"`
	// Use map[string]any or specific structs for images/external/record
	Data map[string]any `json:"-" cbor:"-"`
}

// SelfLabels corresponds to com.atproto.label.defs#selfLabels
type SelfLabels struct {
	Type   string      `json:"$type" cbor:"$type"`
	Values []SelfLabel `json:"values" cbor:"values"`
}

// SelfLabel corresponds to com.atproto.label.defs#selfLabel
type SelfLabel struct {
	Val string `json:"val" cbor:"val"`
}
