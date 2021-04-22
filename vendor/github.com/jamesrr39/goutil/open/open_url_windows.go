// +build windows

package open

import "os/exec"

func OpenURL(url string) error {
	return exec.Command("explorer.exe", url).Start()
}
