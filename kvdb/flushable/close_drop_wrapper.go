package flushable

type closeDropWrapped struct {
	*LazyFlushable
	close func() error
	drop  func()
}

func (w *closeDropWrapped) Close() error {
	return w.close()
}

func (w *closeDropWrapped) RealClose() error {
	return w.LazyFlushable.Close()
}

func (w *closeDropWrapped) Drop() {
	w.drop()
}

func (w *closeDropWrapped) RealDrop() {
	w.LazyFlushable.Drop()
}
