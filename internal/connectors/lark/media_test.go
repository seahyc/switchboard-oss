package lark

import "testing"

// TestLarkMediaRef proves the #48 ref roundtrip + the recovery raw-parse: a rich ref
// encodes→decodes, a bare/empty ref is rejected, and ReextractMediaRef rebuilds the rich
// ref from a stored Lark Message JSON (message_id + body.content carrying the image_key).
func TestLarkMediaRef(t *testing.T) {
	r := larkMediaRef{MsgID: "om_abc", Key: "img_v2_xyz", Type: "image"}
	got, ok := decodeLarkMediaRef(r.encode())
	if !ok || got.MsgID != "om_abc" || got.Key != "img_v2_xyz" || got.Type != "image" {
		t.Fatalf("roundtrip: %+v ok=%v", got, ok)
	}
	if _, ok := decodeLarkMediaRef("img_v2_xyz"); ok {
		t.Fatal("bare key must NOT decode as a rich ref")
	}
	if _, ok := decodeLarkMediaRef(`{"k":"x","t":"image"}`); ok {
		t.Fatal("ref missing message id must NOT decode")
	}

	// Recovery: rebuild from a stored Lark image Message JSON.
	c := &Connector{}
	raw := []byte(`{"message_id":"om_img1","msg_type":"image","body":{"content":"{\"image_key\":\"img_v2_real\"}"}}`)
	ref := c.ReextractMediaRef(raw, "")
	dec, ok := decodeLarkMediaRef(ref)
	if !ok || dec.MsgID != "om_img1" || dec.Key != "img_v2_real" || dec.Type != "image" {
		t.Fatalf("reextract image: %+v ok=%v (ref=%q)", dec, ok, ref)
	}

	// A file message → type "file".
	fraw := []byte(`{"message_id":"om_f1","msg_type":"file","body":{"content":"{\"file_key\":\"file_v2_real\"}"}}`)
	fdec, ok := decodeLarkMediaRef(c.ReextractMediaRef(fraw, ""))
	if !ok || fdec.Type != "file" || fdec.Key != "file_v2_real" {
		t.Fatalf("reextract file: %+v ok=%v", fdec, ok)
	}

	// A text message → no attachment → "".
	if c.ReextractMediaRef([]byte(`{"message_id":"om_t","msg_type":"text","body":{"content":"{\"text\":\"hi\"}"}}`), "") != "" {
		t.Fatal("text message must yield empty ref")
	}
}
