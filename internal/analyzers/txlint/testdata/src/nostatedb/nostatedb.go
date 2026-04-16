package nostatedb

type widget struct {
	value int
}

func mutate(w *widget) {
	w.value++
}
