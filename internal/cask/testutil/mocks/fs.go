// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import (
	cask "github.com/aneshas/gocask/internal/cask"
	mock "github.com/stretchr/testify/mock"
)

// FS is an autogenerated mock type for the FS type
type FS struct {
	mock.Mock
}

// Open provides a mock function with given fields: _a0
func (_m *FS) Open(_a0 string) (cask.File, error) {
	ret := _m.Called(_a0)

	var r0 cask.File
	if rf, ok := ret.Get(0).(func(string) cask.File); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(cask.File)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadFileAt provides a mock function with given fields: _a0, _a1, _a2, _a3
func (_m *FS) ReadFileAt(_a0 string, _a1 string, _a2 []byte, _a3 int64) (int, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3)

	var r0 int
	if rf, ok := ret.Get(0).(func(string, string, []byte, int64) int); ok {
		r0 = rf(_a0, _a1, _a2, _a3)
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string, []byte, int64) error); ok {
		r1 = rf(_a0, _a1, _a2, _a3)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Walk provides a mock function with given fields: _a0, _a1
func (_m *FS) Walk(_a0 string, _a1 func(cask.File) error) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, func(cask.File) error) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewFS interface {
	mock.TestingT
	Cleanup(func())
}

// NewFS creates a new instance of FS. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewFS(t mockConstructorTestingTNewFS) *FS {
	mock := &FS{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
