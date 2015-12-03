package main

import (
	"bytes"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
)

const (
	dbFlagNode           byte = iota
	dbFlagWay            byte = iota
	dbFlagWayNode        byte = iota
	dbFlagRelation       byte = iota
	dbFlagMemberNode     byte = iota
	dbFlagMemberWay      byte = iota
	dbFlagMemberRelation byte = iota
	dbFlagChangeSet      byte = iota
	dbFlagUser           byte = iota
)

type Database struct {
	db *leveldb.DB
}

type Batch struct {
	batch                *leveldb.Batch
	keyWriter, valWriter bytes.Buffer
}

func OpenDatabase(db_file_name string) (db *Database, err error) {
	ldb, err := leveldb.OpenFile(db_file_name, nil)
	if err != nil {
		return
	}
	db = new(Database)
	db.db = ldb
	return
}

func (db *Database) Close() {
	db.db.Close()
}

func (db *Database) StartBatch() (b *Batch) {
	b = new(Batch)
	b.batch = new(leveldb.Batch)
	return
}

func (db *Database) Write(batch *Batch) error {
	return db.db.Write(batch.batch, nil)
}

type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) Write(p []byte) (n int, err error) {
	if ew.err == nil {
		n, ew.err = ew.w.Write(p)
	}
	err = ew.err
	return
}

func (b *Batch) PutNode(id int64, version, lon, lat int32) error {
	k_ew := &errWriter{w: &b.keyWriter}
	v_ew := &errWriter{w: &b.valWriter}

	binary.Write(k_ew, binary.BigEndian, dbFlagNode)
	binary.Write(k_ew, binary.BigEndian, id)
	binary.Write(k_ew, binary.BigEndian, version)
	if k_ew.err != nil {
		return k_ew.err
	}

	binary.Write(v_ew, binary.LittleEndian, lon)
	binary.Write(v_ew, binary.LittleEndian, lat)
	if v_ew.err != nil {
		return v_ew.err
	}

	b.keyWriter.Reset()
	b.valWriter.Reset()

	b.batch.Put(b.keyWriter.Bytes(), b.valWriter.Bytes())

	return nil
}
