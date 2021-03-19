from pyg import periodic_cell, dt

# def test_periodic_cell():
#     c = periodic_cell(lambda a: a + 1, a = 0) 
#     assert c.run() 
#     c = c.go() 
#     assert c.data == 1 
#     assert not c.run() 
    
#     c = c.go(1, time = dt(2000))
#     assert c.updated == dt(2000)
#     assert c.run()
#     assert not c.run(time = dt(2000,1,1,10))

