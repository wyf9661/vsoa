package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	HeaderLength     = 20
	MaxPacketLength  = 262144
	MaxDataLength    = MaxPacketLength - HeaderLength
	MaxQPacketLength = 65507
	MaxQDataLength   = MaxQPacketLength - HeaderLength

	Magic     = 0x9
	Version   = 0x2
	MagicVer  = Magic | (Version << 4)

	TypeServInfo    = 0x00
	TypeRPC         = 0x01
	TypeSubscribe   = 0x02
	TypeUnsubscribe = 0x03
	TypePublish     = 0x04
	TypeDatagram    = 0x05
	TypeQOSSetup    = 0x06
	TypeNoop        = 0xfe
	TypePingEcho    = 0xff

	FlagReply  = 0x1
	FlagTunnel = 0x2
	FlagSet    = 0x4

	padMask  = 0xc0
	padShift = 6

	StatusSuccess       = 0
	StatusPassword      = 1
	StatusArguments     = 2
	StatusInvalidURL    = 3
	StatusNoResponding  = 4
	StatusNoPermissions = 5
	StatusNoMemory      = 6

	MethodGet = 0
	MethodSet = 1
)

type Header struct {
	Type   uint8
	Flags  uint8
	Status uint8
	SeqNo  uint32
	TunID  uint16
}

type Request struct {
	URL    string
	SeqNo  uint32
	Method int
	MWData map[string]any
}

type Payload struct {
	Param any
	Data  []byte
}

type DecodeOptions struct {
	Raw bool
}

type Builder struct {
	header Header
	url    string
	param  []byte
	data   []byte
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Header(typ, flags, status uint8, seqno uint32) *Builder {
	b.header = Header{Type: typ, Flags: flags, Status: status, SeqNo: seqno}
	b.url = ""
	b.param = nil
	b.data = nil
	return b
}

func (b *Builder) URL(url string) *Builder {
	b.url = url
	return b
}

func (b *Builder) TunID(tunid uint16) *Builder {
	b.header.TunID = tunid
	if tunid > 0 {
		b.header.Flags |= FlagTunnel
	} else {
		b.header.Flags &^= FlagTunnel
	}
	return b
}

func EncodeParam(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return append([]byte(nil), x...), nil
	default:
		return json.Marshal(v)
	}
}

func (b *Builder) Payload(p *Payload) error {
	if p == nil {
		b.param = nil
		b.data = nil
		return nil
	}
	param, err := EncodeParam(p.Param)
	if err != nil {
		return err
	}
	b.param = param
	b.data = append([]byte(nil), p.Data...)
	return nil
}

func (b *Builder) Packet() ([]byte, error) {
	urlBytes := []byte(b.url)
	if len(urlBytes)+len(b.param)+len(b.data) > MaxDataLength {
		return nil, fmt.Errorf("payload length too long")
	}
	total := HeaderLength + len(urlBytes) + len(b.param) + len(b.data)
	pad := 0
	if rem := total & 0x3; rem != 0 {
		pad = 4 - rem
		total += pad
	}
	buf := make([]byte, total)
	buf[0] = MagicVer
	buf[1] = b.header.Type
	buf[2] = b.header.Flags | uint8((pad<<padShift)&padMask)
	buf[3] = b.header.Status
	binary.BigEndian.PutUint32(buf[4:8], b.header.SeqNo)
	binary.BigEndian.PutUint16(buf[8:10], b.header.TunID)
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(urlBytes)))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(b.param)))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(b.data)))
	off := HeaderLength
	copy(buf[off:], urlBytes)
	off += len(urlBytes)
	copy(buf[off:], b.param)
	off += len(b.param)
	copy(buf[off:], b.data)
	return buf, nil
}

type Decoded struct {
	Header  Header
	URL     string
	Payload Payload
}

func DecodePacket(packet []byte, opts DecodeOptions) (*Decoded, error) {
	if len(packet) < HeaderLength {
		return nil, errors.New("packet too short")
	}
	if packet[0] != MagicVer {
		return nil, fmt.Errorf("bad packet magic or version: 0x%x", packet[0])
	}
	flags := packet[2]
	pad := int((flags & padMask) >> padShift)
	flags &^= padMask
	urlLen := int(binary.BigEndian.Uint16(packet[10:12]))
	paramLen := int(binary.BigEndian.Uint32(packet[12:16]))
	dataLen := int(binary.BigEndian.Uint32(packet[16:20]))
	total := HeaderLength + urlLen + paramLen + dataLen + pad
	if total > MaxPacketLength {
		return nil, errors.New("packet exceeds max length")
	}
	if len(packet) < total {
		return nil, errors.New("incomplete packet")
	}
	off := HeaderLength
	url := ""
	if urlLen > 0 {
		url = string(packet[off : off+urlLen])
		off += urlLen
	}
	var param any
	if paramLen > 0 {
		raw := append([]byte(nil), packet[off:off+paramLen]...)
		off += paramLen
		if shouldKeepRaw(opts.Raw, packet[1]) {
			param = raw
		} else {
			var decoded any
			if err := json.Unmarshal(raw, &decoded); err == nil {
				param = decoded
			} else {
				param = raw
			}
		}
	}
	var data []byte
	if dataLen > 0 {
		data = append([]byte(nil), packet[off:off+dataLen]...)
	}
	return &Decoded{
		Header: Header{
			Type:   packet[1],
			Flags:  flags,
			Status: packet[3],
			SeqNo:  binary.BigEndian.Uint32(packet[4:8]),
			TunID:  binary.BigEndian.Uint16(packet[8:10]),
		},
		URL: url,
		Payload: Payload{Param: param, Data: data},
	}, nil
}

func shouldKeepRaw(raw bool, typ uint8) bool {
	if !raw {
		return false
	}
	switch typ {
	case TypeServInfo, TypeRPC, TypePublish, TypeDatagram:
		return true
	default:
		return false
	}
}

type Unpacker struct {
	raw    bool
	buf    []byte
	expect int
}

func NewUnpacker(raw bool) *Unpacker {
	return &Unpacker{raw: raw}
}

func (u *Unpacker) Feed(chunk []byte, cb func(*Decoded) error) error {
	u.buf = append(u.buf, chunk...)
	for {
		if len(u.buf) < HeaderLength {
			return nil
		}
		if u.buf[0] != MagicVer {
			return errors.New("bad packet magic or version")
		}
		if u.expect == 0 {
			pad := int((u.buf[2] & padMask) >> padShift)
			urlLen := int(binary.BigEndian.Uint16(u.buf[10:12]))
			paramLen := int(binary.BigEndian.Uint32(u.buf[12:16]))
			dataLen := int(binary.BigEndian.Uint32(u.buf[16:20]))
			u.expect = HeaderLength + urlLen + paramLen + dataLen + pad
			if u.expect > MaxPacketLength {
				return errors.New("packet exceeds max length")
			}
		}
		if len(u.buf) < u.expect {
			return nil
		}
		decoded, err := DecodePacket(u.buf[:u.expect], DecodeOptions{Raw: u.raw})
		if err != nil {
			return err
		}
		if err := cb(decoded); err != nil {
			return err
		}
		u.buf = append([]byte(nil), u.buf[u.expect:]...)
		u.expect = 0
	}
}

func MatchSubscription(sub, url string) bool {
	if sub == "/" {
		return true
	}
	if sub == url {
		return true
	}
	if len(sub) > 1 && sub[len(sub)-1] == '/' {
		prefix := sub[:len(sub)-1]
		if len(url) >= len(prefix) && url[:len(prefix)] == prefix {
			return len(url) == len(prefix) || (len(url) > len(prefix) && url[len(prefix)] == '/')
		}
	}
	return false
}
