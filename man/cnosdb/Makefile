#!/usr/bin/make -f

DESTDIR = ./

MAN1_TXT =
MAN1_TXT += cnosdb.txt
MAN1_TXT += cnosdb-backup.txt
MAN1_TXT += cnosdb-cli.txt
MAN1_TXT += cnosdb-config.txt
MAN1_TXT += cnosdb-restore.txt
MAN1_TXT += cnosdb-run.txt
MAN1_TXT += cnosdb-version.txt

MAN_TXT = $(MAN1_TXT)
MAN_XML = $(patsubst %.txt,%.xml,$(MAN_TXT))

DOC_MAN1 = $(patsubst %.txt,%.1,$(MAN1_TXT))

build: $(DOC_MAN1)

install: build
	@echo '  INSTALL $(DOC_MAN1)' && \
	mkdir -p $(DESTDIR)/share/man/man1 && \
	install -m 0644 $(DOC_MAN1) $(DESTDIR)/share/man/man1

clean:
	rm -f $(MAN_XML) $(DOC_MAN1)

%.xml : %.txt
	@echo '  ASCIIDOC $@' && rm -f $@+ && \
	asciidoc -d manpage -b docbook -o $@+ $< && \
	mv $@+ $@

%.1 : %.xml
	@echo '  XMLTO $@' && \
	xmlto man $< 2> /dev/null

.PHONY: build install clean