.PHONY: all clean coordinator frontend frontend_lb kvstore smtp_server

all: coordinator frontend frontend_lb kvstore smtp_server

coordinator:
	$(MAKE) -C coordinator

frontend:
	$(MAKE) -C frontend

frontend_lb:
	$(MAKE) -C frontend_lb

kvstore:
	$(MAKE) -C kvstore

smtp_server:
	$(MAKE) -C smtp_server

clean:
	$(MAKE) -C coordinator clean
	$(MAKE) -C frontend clean
	$(MAKE) -C frontend_lb clean
	$(MAKE) -C kvstore clean
	$(MAKE) -C smtp_server clean
