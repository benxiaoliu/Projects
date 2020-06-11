# -*- coding: utf-8 -*-
import uuid

name = "test_name"

#print uuid.uuid1()
print uuid.uuid3(uuid.NAMESPACE_URL, name)
#print uuid.uuid4()
#print uuid.uuid5(uuid.NAMESPACE_DNS, name)