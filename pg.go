package pgsimple

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	SSLRequestCode int32 = 80877103
)

type PgMessageID byte

const (
	//CancelRequest is wacky
	//Startup is wacky
	//SSLRequest is wacky
	Authentication           PgMessageID = 'R'
	BackendKeyData           PgMessageID = 'K'
	Bind                     PgMessageID = 'B'
	BindComplete             PgMessageID = '2'
	CommandComplete          PgMessageID = 'C'
	CloseComplete            PgMessageID = '3'
	CopyData                 PgMessageID = 'd'
	CopyDone                 PgMessageID = 'c'
	CopyFail                 PgMessageID = 'f'
	CopyInResponse           PgMessageID = 'G'
	CopyOutResponse          PgMessageID = 'H' //or Flush
	CopyBoth                 PgMessageID = 'W'
	DataRow                  PgMessageID = 'D' // or Describe
	EmptyQueryResponse       PgMessageID = 'I' //
	Execute                  PgMessageID = 'E' // or ErrorResposne
	FunctionCall             PgMessageID = 'F'
	FunctionCallResponse     PgMessageID = 'V'
	NegotiateProtocolVersion PgMessageID = 'v'
	NoData                   PgMessageID = 'n'
	NoticeResponse           PgMessageID = 'N'
	NotificationResponse     PgMessageID = 'A'
	ParameterDescription     PgMessageID = 't'
	ParameterStatus          PgMessageID = 'S'
	Parse                    PgMessageID = 'P' //
	ParseComplete            PgMessageID = '1' //
	PasswordMessage          PgMessageID = 'p' //
	PortalSuspended          PgMessageID = 's' //
	Query                    PgMessageID = 'Q' //
	ReadyForQuery            PgMessageID = 'Z' //
	RowDescription           PgMessageID = 'T' //
	Terminate                PgMessageID = 'X' //
)

func (p PgMessageID) String() string {
	switch p {
	case Authentication:
		return "authentication..."
	case BackendKeyData:
		return "BackendKeyData"
	case Bind:
		return "Bind"
	case BindComplete:
		return "BindComplete"
	case CommandComplete:
		return "Close or Command Complete"
	case CloseComplete:
		return "CloseComplete"
	case CopyData:
		return "CopyData"
	case CopyDone:
		return "CopyDone"
	case CopyFail:
		return "CopyFail"
	case CopyInResponse:
		return "CopyInResponse"
	case CopyOutResponse:
		return "CopyOutResponse or Flush	"
	case CopyBoth:
		return "CopyBoth"
	case DataRow:
		return "DataRow or Describe"
	case EmptyQueryResponse:
		return "EmptyQueryResponse"
	case Execute:
		return "ErrorResponse or Execute"
	case FunctionCall:
		return "FunctionCall"
	case FunctionCallResponse:
		return "FunctionCallResponse"
	case NegotiateProtocolVersion:
		return "NegotiateProtocolVersion"
	case NoData:
		return "NoData"
	case NoticeResponse:
		return "NoticeResponse"
	case NotificationResponse:
		return "NotificationResponse"
	case ParameterDescription:
		return "ParameterDescription"
	case ParameterStatus:
		return "ParameterStatus"
	case Parse:
		return "Parse"
	case ParseComplete:
		return "ParseComplete"
	case PasswordMessage:
		return "PasswordMessage"
	case PortalSuspended:
		return "PortalSuspended"
	case Query:
		return "Query"
	case ReadyForQuery:
		return "ReadyForQuery"
	case RowDescription:
		return "RowDescription"
	case Terminate:
		return "Terminate"
	default:
		return "Unknown"
	}
}
func Decode(b PgMessageID, data []byte) {
	fmt.Println("Decode: ", b, data)
	switch b {
	case Parse:
		offset := 0
		//		l := makeInt32(data[:offset])
		i := bytes.IndexByte(data[offset:], 0)
		var name string
		var query string
		if i < 0 {
			//no name
		} else {
			name = string(data[offset:i])
			fmt.Println("Name:", name)
			offset += i + 1
		}
		i = bytes.IndexByte(data[offset:], 0)
		if i < 0 {
			//no query
		} else {
			query = string(data[offset : i+1])
			fmt.Println("Query:", query)
			offset += i + 1
		}
		numIDS := makeInt16(data[offset:])
		fmt.Printf("%s|%s|%d\n", name, query, numIDS)

	}
}

var (
	Null = []byte{0}
)

type FrameBuffer struct {
	frame bytes.Buffer
}

func (f *FrameBuffer) Dump() {
	fmt.Println("")
	for _, b := range f.Bytes() {
		fmt.Printf("%x:", b)
	}
	fmt.Println("")
}
func (f *FrameBuffer) AddByte(b byte) error {
	return f.frame.WriteByte(b)
}

func (f *FrameBuffer) AddInt16(i int16) (int, error) {
	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(i))
	return f.frame.Write(x)
}

func (f *FrameBuffer) AddInt32(i int32) (int, error) {
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(i))
	return f.frame.Write(x)
}

func (f *FrameBuffer) AddBytes(b []byte) (int, error) {
	for i := range b {
		f.frame.WriteByte(b[i])
	}
	return len(b), nil
}
func (f *FrameBuffer) AddString(s string) (int, error) {
	n, err := f.frame.WriteString(s + "\x00")
	if err != nil {
		return 0, err
	}
	return n, nil
}
func (f *FrameBuffer) Bytes() []byte {
	return f.frame.Bytes()
}
func (f *FrameBuffer) Reset() {
	f.frame.Reset()
}

type pgHandler struct {
	client      net.Conn
	readBuffer  []byte
	sizeTmp     []byte
	brd         *bufio.Reader
	writeBuffer FrameBuffer
	params      map[string]string
}

func NewHandler(client net.Conn) *pgHandler {
	p := &pgHandler{
		client:     client,
		readBuffer: make([]byte, 4096),
		brd:        bufio.NewReader(client),
		params:     make(map[string]string),
		sizeTmp:    make([]byte, 4),
	}
	return p
}

func makeInt32(b []byte) int32 {
	var code int32
	//lenght must be 4 bytes
	reader := bytes.NewReader(b[:4])
	binary.Read(reader, binary.BigEndian, &code)
	return code
}
func makeInt16(b []byte) int16 {
	var code int16
	//lenght must be 4 bytes
	reader := bytes.NewReader(b[:2])
	binary.Read(reader, binary.BigEndian, &code)
	return code
}
func UpgradeServerConnection(client net.Conn) net.Conn {
	//	creds := config.GetCredentials()

	tlsConfig := tls.Config{}

	cert, _ := tls.LoadX509KeyPair(
		"/Users/toddgruben/go/src/github.com/pilosa/simple/out/PGAuth.crt",
		"/Users/toddgruben/go/src/github.com/pilosa/simple/out/PGAuth.key")

	tlsConfig.Certificates = []tls.Certificate{cert}

	client = tls.Server(client, &tlsConfig)

	return client
}

//perform a simple startup
// connect set to no ssl or authentication
func (pgh *pgHandler) Startup() error {
	//_, err := pgh.client.Read(pgh.readBuffer)
	_, err := io.ReadFull(pgh.brd, pgh.sizeTmp[:])
	if err != nil {
		return err
	}
	size := int(binary.BigEndian.Uint32(pgh.sizeTmp[:])) - 4
	fmt.Println("startup size", size)

	fmt.Println("ReadSomeMore")
	if cap(pgh.readBuffer) < size {
		pgh.readBuffer = make([]byte, size)
	} else {
		pgh.readBuffer = pgh.readBuffer[:size]
	}
	_, err = io.ReadFull(pgh.brd, pgh.readBuffer)
	if err != nil {
		return err
	}
	version := makeInt32(pgh.readBuffer[0:4])
	fmt.Println("STARTUP", version)
	if version == SSLRequestCode {
		fmt.Println("CHANGING TO SSL")
		//pgh.client.Write([]byte{'N'}) //SSLNotAllowed
		pgh.client.Write([]byte{'S'}) //SSLAllowed
		pgh.client = UpgradeServerConnection(pgh.client)
		if _, err := pgh.client.Read(pgh.readBuffer); err == io.EOF {
			return err
		}
	}
	fmt.Println("GOT", pgh.readBuffer)
	parts := bytes.Split(pgh.readBuffer[4:], Null)
	i := 0
	for i < len(parts) {
		if len(parts[i]) > 0 {
			pgh.params[string(parts[i])] = string(parts[i+1])
		} else {
			break
		}
		i += 2
	}
	fmt.Println(pgh.params)
	fmt.Println("AUTH OK")
	// auth ok
	pgh.writeBuffer.AddByte(byte(Authentication)) //authentaction OK
	pgh.writeBuffer.AddInt32(int32(8))
	pgh.writeBuffer.AddInt32(int32(0))
	pgh.client.Write(pgh.writeBuffer.Bytes())
	//
	pgh.SetReadyForQuery()
	return nil
}
func (pgh *pgHandler) ReadPacket() (PgMessageID, []byte, error) {
	b, err := pgh.brd.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	ptype := PgMessageID(b)
	_, err = io.ReadFull(pgh.brd, pgh.sizeTmp[:])
	if err != nil {
		return ptype, nil, err
	}
	size := int(binary.BigEndian.Uint32(pgh.sizeTmp[:]))
	// size includes itself.
	size -= 4
	if cap(pgh.readBuffer) < size {
		pgh.readBuffer = make([]byte, size)
	} else {
		pgh.readBuffer = pgh.readBuffer[:size]
	}
	_, err = io.ReadFull(pgh.brd, pgh.readBuffer)
	if err != nil {
		return 0, nil, err
	}
	return ptype, pgh.readBuffer, err
}
func (pgh *pgHandler) Send(rs ResultSet) error {
	pgh.writeBuffer.Reset()
	rs.WriteTo(&pgh.writeBuffer) //write out the header
	pgh.client.Write(pgh.writeBuffer.Bytes())
	for row, next := rs.iterator(); next != nil; row, next = next() {
		pgh.writeBuffer.Reset()
		row.WriteTo(&pgh.writeBuffer)
		pgh.client.Write(pgh.writeBuffer.Bytes())
	}
	//write the data
	pgh.SendCommandComplete("SELECT")
	pgh.SetReadyForQuery()
	return nil
}
func (pgh *pgHandler) Shutdown() {
	pgh.client.Close()
}
func (pgh *pgHandler) SendCommandComplete(tag string) {
	pgh.writeBuffer.Reset()
	pgh.writeBuffer.AddByte('C') //ready for query
	pgh.writeBuffer.AddInt32(int32(len(tag) + 5))
	pgh.writeBuffer.AddString(tag)
	pgh.client.Write(pgh.writeBuffer.Bytes())
}
func (pgh *pgHandler) SetReadyForQuery() {
	pgh.writeBuffer.Reset()
	pgh.writeBuffer.AddByte('Z') //ready for query
	pgh.writeBuffer.AddInt32(int32(5))
	pgh.writeBuffer.AddByte('I')
	pgh.client.Write(pgh.writeBuffer.Bytes())
}

type RowDescriptionMessage struct {
	fieldName    string
	tableID      int32 //either a table/col id or 0
	fieldID      int16 //either a table/col id or 0
	typeID       int32 //field type
	typeLen      int16 //size in bytes of field
	typeModifier int32 //type modifer?
	mode         int16 //0=text 1=binary
}

func (rd *RowDescriptionMessage) writeTo(b *FrameBuffer) {
	b.AddString(rd.fieldName)
	b.AddInt32(rd.tableID)
	b.AddInt16(rd.fieldID)
	b.AddInt32(rd.typeID)
	b.AddInt16(rd.typeLen)
	b.AddInt32(rd.typeModifier)
	b.AddInt16(rd.mode)
}

type ResultSet struct {
	header   []RowDescriptionMessage
	iterator DataRowIterator
}

func (rs *ResultSet) addColumn(r RowDescriptionMessage) {
	rs.header = append(rs.header, r)
}

func (rs *ResultSet) WriteTo(out *FrameBuffer) {
	var buff FrameBuffer
	for i := range rs.header {
		rs.header[i].writeTo(&buff)
	}
	extra := buff.Bytes()
	out.Reset()
	out.AddByte('T')
	out.AddInt32(6 + int32(len(extra)))
	out.AddInt16(int16(len(rs.header)))
	out.AddBytes(extra)
}

// just some thre
type DataRowMaker interface {
	WriteTo(buf *FrameBuffer)
}
type DataRowIterator func() (row DataRowMaker, next DataRowIterator)
