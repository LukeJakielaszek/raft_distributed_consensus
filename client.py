import rpyc

while True:
    con = rpyc.connect('localhost', 5001)
    print(con.root.is_leader())
    con.root.request_vote()
    con.root.append_entries(1, 3)
    con.close()
