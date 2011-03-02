.PHONY: all clean

all:
	ocamlbuild -use-ocamlfind all.otarget

clean:
	ocamlbuild -clean

