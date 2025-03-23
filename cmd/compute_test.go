/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"math"
	"testing"
)

func TestReturnsToCagr(t *testing.T) {
	type args struct {
		r  float64
		yr float64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "Test case for 2 yr",
			args: args{r: 20.33870332, yr: 2},
			want: 9.698998773,
		},
		{
			name: "Test case for 3 yr",
			args: args{r: 79.23166788, yr: 3},
			want: 21.47071339,
		},
		{
			name: "Test case for 4 yr",
			args: args{r: 132.3640603, yr: 4},
			want: 23.46453901,
		},
		{
			name: "Test case for 5 yr",
			args: args{r: 78.93324431, yr: 5},
			want: 12.34097979,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := ReturnsToCagr(tt.args.r, tt.args.yr); math.Round(got) != math.Round(tt.want) {
				t.Errorf("ReturnsToCagr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_calculateReturnsWithMonthData(t *testing.T) {
	type args struct {
		returns []float64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "Test case for 1 yr",
			args: args{returns: []float64{
				0.48,
				0.98,
				0.65,
				-0.98,
				-0.12,
				-0.49,
				-2.69,
				8.17,
				5.7,
				-3.6,
				8.8,
				-0.86}},
			want: 16.27,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateReturnsWithMonthData(tt.args.returns); math.Round(got) != math.Round(tt.want) {
				t.Errorf("calculateReturnsWithMonthData() = %v, want %v", got, tt.want)
			}
		})
	}
}
