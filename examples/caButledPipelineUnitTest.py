#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
Run with:
or
   python
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
from lsst.pex.harness import Dataset
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.harness.IOStage
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.afw.image as afwImage
import lsst.afw.display.ds9 as ds9
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom
import lsst.afw.cameraGeom.utils as cameraGeomUtils
import lsst.meas.algorithms.defects as measDefects

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
    type(writeFile)
except NameError:
    display = False
    writeFile = True

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
class IsrCcdAssemblyTestCase(unittest.TestCase):
    """A test case for Isr Pipeline using Simple Stage Tester"""

    def setUp(self):
        afwDataDir = eups.productDir("afwdata")
        self.root = afwDataDir+"/ImSim"
        if not os.path.exists("registry.sqlite3"):
            os.symlink(os.path.join(self.root, "registry.sqlite3"),
                    "./registry.sqlite3")
        self.clipboard = pexClipboard.Clipboard()         
        self.clipboard.put('jobIdentity', {
                'visit': 85751839, 'snap': 0,
                'raft': "2,3", 'sensor': "1,1", 'filter': "r"
            })
        self.clipboard.put('inputDatasets', [
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,0", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,1", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,2", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,3", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,4", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,5", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,6", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,7", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,0", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,1", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,2", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,3", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,4", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,5", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,6", filter="r"),
                Dataset("postISR", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="1,7", filter="r")
            ])
    def tearDown(self):
        for key in self.__dict__.keys():
            del self.__dict__[key]
    def testPipe(self):
        """Pipeline test case."""
        try:
            ps = dafBase.PropertySet() 
            ps.set("input", self.root);
            dafPersist.LogicalLocation.setLocationMap(ps)
            p = pexPolicy.Policy()
            p0 = pexPolicy.Policy.createPolicy(eups.productDir("ip_pipeline")+"/tests/" +"CaInputStage.paf")
            s0 = lsst.pex.harness.IOStage.InputStage(p0)
            t0 = SimpleStageTester(s0)

            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                 "IsrCcdAssemblyStageDictionary.paf", "policy")
            p1 = pexPolicy.Policy.createPolicy(file)
            s1 = ipPipe.IsrCcdAssemblyStage(p)
            t1 = SimpleStageTester(s1)

            o0 = t0.runWorker(self.clipboard)
            o1 = t1.runWorker(o0)
            exposure = o1.get(p1.get("outputKeys.assembledCcdExposure"))
        except Exception, e:
            print e
        if writeFile:
            exposure.writeFits("Exposure.fits")

        if display:
            ds9.mtv(exposure, frame=5, title="Output")

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    elif not eups.productDir("obs_lsstSim"):
        print >> sys.stderr, "obs_lsstSim not set up; skipping test"
    else:        
        suites += unittest.makeSuite(IsrCcdAssemblyTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
