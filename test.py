from jsonpath_rw import parse, jsonpath

x = {'a': {'b': {'text': 'hey'}, 'c': {'text': 'hey2222'}}}


def test(seg):
  seg['tokens'] = 'test333'
  seg['crf'] = 'something'


jsonpath_expr = parse('a.*.text.`parent`')
matches = jsonpath_expr.find(x)

for i in matches:
  val = i.value
  test(val)

print "---- final"
print x

# print [i.value for i in matches]
