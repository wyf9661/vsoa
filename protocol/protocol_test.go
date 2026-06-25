package protocol

import (
	"bytes"
	"testing"
)

func TestPacketRoundTrip(t *testing.T) {
	b := NewBuilder().Header(TypeRPC, FlagSet, 0, 123).TunID(77).URL("/echo")
	if err := b.Payload(&Payload{Param: map[string]any{"hello": "world"}, Data: []byte{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	pkt, err := b.Packet()
	if err != nil {
		t.Fatal(err)
	}
	d, err := DecodePacket(pkt, DecodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Header.Type != TypeRPC || d.Header.SeqNo != 123 || d.Header.TunID != 77 {
		t.Fatalf("unexpected header: %+v", d.Header)
	}
	if d.URL != "/echo" {
		t.Fatalf("unexpected url: %s", d.URL)
	}
	m := d.Payload.Param.(map[string]any)
	if m["hello"] != "world" {
		t.Fatalf("unexpected param: %#v", d.Payload.Param)
	}
	if !bytes.Equal(d.Payload.Data, []byte{1, 2, 3}) {
		t.Fatalf("unexpected data: %v", d.Payload.Data)
	}
}

func TestRawModeKeepsBytes(t *testing.T) {
	b := NewBuilder().Header(TypeRPC, 0, 0, 1).URL("/raw")
	if err := b.Payload(&Payload{Param: []byte(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	pkt, _ := b.Packet()
	d, err := DecodePacket(pkt, DecodeOptions{Raw: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.Payload.Param.([]byte); !ok {
		t.Fatalf("expected raw bytes, got %#v", d.Payload.Param)
	}
}

func TestUnpackerSplitFeed(t *testing.T) {
	b := NewBuilder().Header(TypePingEcho, FlagReply, 0, 9)
	pkt, _ := b.Packet()
	u := NewUnpacker(false)
	called := 0
	if err := u.Feed(pkt[:5], func(*Decoded) error { called++; return nil }); err != nil {
		t.Fatal(err)
	}
	if called != 0 {
		t.Fatalf("callback should not run yet")
	}
	if err := u.Feed(pkt[5:], func(*Decoded) error { called++; return nil }); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("callback count = %d", called)
	}
}

func TestSubscriptionMatch(t *testing.T) {
	cases := []struct {
		sub, url string
		want     bool
	}{
		{"/", "/a/b", true},
		{"/a", "/a", true},
		{"/a/", "/a", true},
		{"/a/", "/a/b", true},
		{"/a/", "/ab", false},
	}
	for _, tc := range cases {
		if got := MatchSubscription(tc.sub, tc.url); got != tc.want {
			t.Fatalf("match(%q,%q)=%v want %v", tc.sub, tc.url, got, tc.want)
		}
	}
}
