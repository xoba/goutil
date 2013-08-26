// +build linux

package goutil

import (
	"fmt"
	"syscall"
	"time"
)

type InotifyMessage struct {
	Path    string
	Error   error
	Exiting bool
	Quit    chan bool
}

// not working well yet
func Inotify(path string, debounce time.Duration, quit chan bool, ch chan InotifyMessage) error {

	fd, errno := syscall.InotifyInit()
	if fd == -1 {
		return errno
	}

	var mask uint32
	mask |= syscall.IN_ATTRIB

	if false {
		mask |= syscall.IN_MOVED_TO
		mask |= syscall.IN_MOVED_FROM
		mask |= syscall.IN_CREATE
		mask |= syscall.IN_ATTRIB
		mask |= syscall.IN_MODIFY
		mask |= syscall.IN_MOVE_SELF
		mask |= syscall.IN_DELETE
		mask |= syscall.IN_DELETE_SELF
		mask |= syscall.IN_EXCL_UNLINK
	}

	wd, errno := syscall.InotifyAddWatch(fd, path, mask)
	if wd == -1 {
		return errno
	}

	raw := make(chan bool)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := syscall.Read(fd, buf)
			if n <= 0 || err != nil {
				fmt.Println("*1", n, err)
				raw <- false
				break
			} else {
				fmt.Println("*2")
				raw <- true
			}
		}
		close(raw)
	}()

	go func() {

		for {
			select {

			case ok := <-raw:
				fmt.Println("*3")
				if ok {
					fmt.Println("*6")
					ch <- InotifyMessage{
						Path: path,
						Quit: quit,
					}

				} else {
					fmt.Println("*5")
					ch <- InotifyMessage{
						Path:    path,
						Exiting: true,
						Quit:    quit,
					}
					break
				}

			case <-quit:
				fmt.Println("*4")
				err := syscall.Close(fd)
				ch <- InotifyMessage{
					Path:    path,
					Error:   err,
					Exiting: true,
					Quit:    quit,
				}
				break
			}
		}

		fmt.Println("*99")
	}()

	return nil
}
