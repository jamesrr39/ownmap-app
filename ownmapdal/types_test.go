package ownmapdal

import (
	"reflect"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
)

func TestParseDBConnFilePath(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name  string
		args  args
		want  DBFileConnectionURL
		want1 errorsx.Error
	}{
		{
			args: args{"postgresql://localhost"},
			want: DBFileConnectionURL{
				Type:           "postgresql",
				ConnectionPath: "localhost",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ParseDBConnFilePath(tt.args.str)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseDBConnFilePath() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseDBConnFilePath() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
