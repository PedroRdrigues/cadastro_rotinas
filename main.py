from _rotinas import Rotinas

try:
    rotinas = Rotinas()
    rotinas.run()
except Exception as e:
    print("erro no arquivo principal: ",e)
