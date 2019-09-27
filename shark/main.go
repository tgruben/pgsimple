package main

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"log"
	"net"
	. "github.com/logrusorgru/aurora"
)


func localAddresses() {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Print(fmt.Errorf("localAddresses: %v\n", err.Error()))
		return
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			log.Print(fmt.Errorf("localAddresses: %v\n", err.Error()))
			continue
		}
		for _, a := range addrs {
			log.Printf("%v %v\n", i.Name, a)
		}
	}
}

func main() {
	localAddresses()
	//        if true {
	//	   return
	//	}
	device := "lo0"
	snapshotLen := int32(4096)
	promiscuous := true
	filter := "tcp and port 5432"

	// Open device
	handle, err := pcap.OpenLive(device, snapshotLen, promiscuous, -1)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()
	err = handle.SetBPFFilter(filter)
	if err != nil {
		log.Fatal(err)
	}
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	pchan := packetSource.Packets()
        color:=Green
	for {
		select {
		case packet, ok := <-pchan:
			if !ok {
				fmt.Println("Channel closed by device")
				return
			}
			fmt.Println(packet)
			layer := packet.TransportLayer()
			lp :=layer.LayerPayload()
			fmt.Println("CHECK", layer.LayerType())
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			tcp, _ := tcpLayer.(*layers.TCP)

			// TCP layer variables:
			// SrcPort, DstPort, Seq, Ack, DataOffset, Window, Checksum, Urgent
			// Bool flags: FIN, SYN, RST, PSH, ACK, URG, ECE, CWR, NS
			if tcp.SrcPort==5432{
				color= Green			//server to client
			}else{
				color = Red
			}

			if len(lp) > 0 {
				fmt.Println(color(lp))
				for _,c:=range lp{
				if c>30{
					fmt.Print(color(string(c)))
				}else{
					fmt.Print(color(c))
				}

				}
			}
			fmt.Println()

			//		case <-quit:
			//			return
		}
	}
}
