package server

import (
	"log"
	"net"
	"syscall"

	"redis/config"
	"redis/core"
)

var con_clients int = 0

func RunAsyncTCPServer() error {
	log.Println("starting a asynchronous TCP server on", config.Host, config.Port)

	max_clients := 20000

	var events []syscall.Kevent_t = make([]syscall.Kevent_t, max_clients)

	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}

	defer syscall.Close(serverFD)

	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	ip4 := net.ParseIP(config.Host)
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return err
	}

	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// async io starts here

	epollFD, err := syscall.Kqueue()
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Close(epollFD)

	// Specify the events we want to get hints about
	// and set the socket on which
	var socketServerEvent syscall.Kevent_t = syscall.Kevent_t{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	// Listen to read events on the Server itself
	if _, err = syscall.Kevent(epollFD, []syscall.Kevent_t{socketServerEvent}, nil, nil); err != nil {
		return err
	}

	for {
		// see if any FD is ready for an IO
		nevents, e := syscall.Kevent(epollFD, nil, events[:], nil)
		if e != nil {
			continue
		}

		for i := 0; i < nevents; i++ {
			// if the socket server itself is ready for an IO
			if int(events[i].Ident) == serverFD {
				// accept the incoming connection from a client
				fd, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("err", err)
					continue
				}

				// increase the number of concurrent clients count
				con_clients++
				syscall.SetNonblock(serverFD, true)

				// add this new TCP connection to be monitored
				var socketClientEvent syscall.Kevent_t = syscall.Kevent_t{
					Ident:  uint64(fd),
					Filter: syscall.EVFILT_READ,
					Flags:  syscall.EV_ADD,
				}

				if _, err = syscall.Kevent(epollFD, []syscall.Kevent_t{socketClientEvent}, nil, nil); err != nil {
					log.Fatal(err)
				}
			} else {
				comm := core.FDComm{Fd: int(events[i].Ident)}
				cmd, err := readCommand(comm)
				if err != nil {
					syscall.Close(int(events[i].Ident))
					con_clients -= 1
					continue
				}
				respond(cmd, comm)
			}
		}
	}
}
