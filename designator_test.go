package triflestats

import "testing"

func TestLinearDesignator(t *testing.T) {
	d := NewLinearDesignator(0, 100, 10)

	if got := d.Designate(50); got != "50" {
		t.Fatalf("expected 50, got %s", got)
	}
	if got := d.Designate(0); got != "0" {
		t.Fatalf("expected 0, got %s", got)
	}
	if got := d.Designate(100); got != "100" {
		t.Fatalf("expected 100, got %s", got)
	}
	if got := d.Designate(101); got != "100+" {
		t.Fatalf("expected 100+, got %s", got)
	}
	if got := d.Designate(12.3); got != "20" {
		t.Fatalf("expected 20, got %s", got)
	}
}

func TestCustomDesignator(t *testing.T) {
	d := NewCustomDesignator([]float64{0, 10, 25, 50, 100})

	if got := d.Designate(30); got != "50" {
		t.Fatalf("expected 50, got %s", got)
	}
	if got := d.Designate(5); got != "10" {
		t.Fatalf("expected 10, got %s", got)
	}
	if got := d.Designate(75); got != "100" {
		t.Fatalf("expected 100, got %s", got)
	}
	if got := d.Designate(120); got != "100+" {
		t.Fatalf("expected 100+, got %s", got)
	}
}

func TestGeometricDesignator(t *testing.T) {
	d := NewGeometricDesignator(1, 2)
	if got := d.Designate(16); got != "2.0+" {
		t.Fatalf("expected 2.0+, got %s", got)
	}

	d2 := NewGeometricDesignator(1, 200)
	if got := d2.Designate(125); got != "1000.0" {
		t.Fatalf("expected 1000.0, got %s", got)
	}

	d3 := NewGeometricDesignator(0, 1)
	if got := d3.Designate(0.004); got != "0.01" {
		t.Fatalf("expected 0.01, got %s", got)
	}
}
