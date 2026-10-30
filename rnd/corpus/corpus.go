package corpus

import "fmt"

var words = []string{"main", "index", "utils", "config", "test", "report", "data", "model", "train", "notes",
	"readme", "build", "server", "client", "handler", "schema", "image", "audio", "meeting", "draft",
	"summary", "export", "backup", "module", "service", "query", "table", "chart", "result", "input"}

var exts = []string{"go", "py", "md", "json", "txt", "csv", "png", "js", "ts", "yaml", "log", "wav", "pdf", "parquet", "ipynb", "html"}

func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func FileName(random bool, seq, k int) string {
	if !random {
		return fmt.Sprintf("f%05d", k)
	}
	h := mix(uint64(seq))
	w := words[h%uint64(len(words))]
	e := exts[(h>>8)%uint64(len(exts))]
	switch (h >> 16) % 4 {
	case 0:
		return fmt.Sprintf("%s%d.%s", w, k, e)
	case 1:
		return fmt.Sprintf("%s_%d.%s", w, k, e)
	case 2:
		return fmt.Sprintf("%s-%x-%d.%s", w, h>>24&0xffffff, k, e)
	default:
		return fmt.Sprintf("%s_%s_%x%d.%s", w, words[(h>>40)%uint64(len(words))], h>>48&0xfff, k, e)
	}
}

func DirName(random bool, user, dir int) string {
	if !random {
		return fmt.Sprintf("d%04d", dir)
	}
	h := mix(uint64(user)<<32 | uint64(dir))
	return fmt.Sprintf("%s-%04d", words[h%uint64(len(words))], dir)
}

func FileSize(random bool, seq int) uint32 {
	if !random {
		return 4 << 10
	}
	h := mix(uint64(seq) ^ 0x5bd1e995)
	switch p := h % 100; {
	case p < 60:
		return uint32(64 + h>>8%(4<<10))
	case p < 90:
		return uint32(4<<10 + h>>8%(60<<10))
	default:
		return uint32(64<<10 + h>>8%(4<<20-64<<10))
	}
}
