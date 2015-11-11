package redis

import (
	"fmt"
	"testing"
)

func TestInheritance(t *testing.T) {
	bar := Bar{}
	t.Logf(bar.Bla())
}

type Foo int

func (f *Foo) Bla() string {
	fmt.Println("Called Bla()!")
	return "called Bla()"
}

type Bar struct {
	Foo
	offset int64
}

func (b *Bar) Whoop() string {
	return "called Whoop()"
}

func (b *Bar) Modify() {
	b.offset = 10
	fmt.Println(&b.offset)
}

func (b *Bar) doTest() {
	fmt.Println(&b.offset)
	b.Modify()
	fmt.Println(&b.offset)
}

func TestModify(t *testing.T) {
	bar := Bar{}
	bar.doTest()
}
