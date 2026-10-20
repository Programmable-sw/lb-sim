import subprocess,tempfile,unittest
from pathlib import Path
BASE=Path(__file__).resolve().parents[1]
class CppTests(unittest.TestCase):
 def test_cc_and_ops(self):
  with tempfile.TemporaryDirectory(prefix='v7g-unit-')as d:
   for name in('test_common_nack.cpp','test_ops_random.cpp'):
    with self.subTest(test=name):
     exe=Path(d)/name.replace('.cpp','')
     subprocess.run(['g++','-std=c++11','-I'+str(BASE/'src'),str(BASE/'tests'/name),'-o',str(exe)],check=True)
     subprocess.run([str(exe)],check=True)
if __name__=='__main__':unittest.main()
