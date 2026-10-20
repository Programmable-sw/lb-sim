"""Build the current source directly, without historical result dependencies."""
import argparse,hashlib,json,subprocess
from pathlib import Path
BASE=Path(__file__).resolve().parent
ROOT=BASE.parents[2]
SRC=BASE/'src';BUILD=BASE/'.build';BIN=BUILD/'htsim_v7g'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--jobs',type=int,default=2);a=p.parse_args()
 assert a.jobs>0
 subprocess.run(['make','-C',str(ROOT/'sim'),'-j',str(a.jobs),'libhtsim.a'],check=True)
 BUILD.mkdir(exist_ok=True)
 cmd=['g++','-std=c++11','-O2','-I'+str(SRC),'-I'+str(ROOT/'sim'),'-I'+str(ROOT/'sim/datacenter'),str(SRC/'v7g_hybrid_b.cpp'),str(ROOT/'sim/libhtsim.a'),'-o',str(BIN)]
 subprocess.run(cmd,check=True,cwd=ROOT)
 identity=dict(binary_sha256=sha(BIN),library_sha256=sha(ROOT/'sim/libhtsim.a'),command=cmd,
  source_files={p.name:sha(p)for p in SRC.iterdir()if p.is_file()})
 (BUILD/'BUILD_IDENTITY.json').write_text(json.dumps(identity,indent=2)+'\n')
 print(BIN,identity['binary_sha256'])
if __name__=='__main__':main()
