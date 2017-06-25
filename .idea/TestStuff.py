class MyMutableInt:
    def __init__(self,i):
        self.i=i;
def produceIncBy(x):
    def incBy(y):
        nonlocal x; #like global, except for the enclosing scope
        x.i=2*x.i;
        return y+x.i
    return incBy
z=MyMutableInt(5)
incBy5=produceIncBy(z)
z=MyMutableInt(6)
incBy6=produceIncBy(z)
print(incBy5(0)) #10 = 2*5
print(z.i)
z.i=20;
print(z.i)
print(incBy5(0)) #20 = 2*2*5
print(incBy5(0)) #40 = 2*2*2*5
print(incBy6(0)) #40 = 2*20
print(incBy6(0)) #80 = 2*2*20