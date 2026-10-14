"""Exercise the real approval configuration without application startup side effects."""
import ast
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from flask import Flask
ROOT=Path(__file__).resolve().parents[1]
def configured(value):
    tree=ast.parse((ROOT/'config.py').read_text(encoding='utf-8-sig'))
    helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_to_bool')
    config=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Config')
    assignment=next(n for n in config.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='PM_OWNER_WRITES_APPROVED' for t in n.targets))
    isolated=ast.Module(body=[helper,ast.ClassDef(name='Config',bases=[],keywords=[],body=[assignment],decorator_list=[])],type_ignores=[])
    ast.fix_missing_locations(isolated)
    namespace={'os':os}
    environment={} if value is None else {'PM_OWNER_WRITES_APPROVED':value}
    with patch.dict(os.environ,environment,clear=True):
        exec(compile(isolated,str(ROOT/'config.py'),'exec'),namespace)
    app=Flask(__name__);app.config.from_object(namespace['Config'])
    return app.config['PM_OWNER_WRITES_APPROVED']
class OwnerApprovalConfigTests(unittest.TestCase):
    def test_default_and_invalid_values_are_boolean_false(self):
        for value in (None,'','false','0','off','no','typo'):
            with self.subTest(value=value):self.assertIs(configured(value),False)
    def test_explicit_true_values_are_boolean_true(self):
        for value in ('true','TRUE',' true ','1','yes','on'):
            with self.subTest(value=value):self.assertIs(configured(value),True)
if __name__=='__main__':unittest.main()
