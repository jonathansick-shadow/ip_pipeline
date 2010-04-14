#!/usr/bin/env python
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
    writeFile = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
class IsrCcdAssemblyTestCase(unittest.TestCase):
    """A test case for Isr Pipeline using Simple Stage Tester"""

    def setUp(self):
        afwDataDir = eups.productDir("afwdata")
        self.datadir = afwDataDir+"/ImSim"
        self.clipboard = pexClipboard.Clipboard()         
        self.clipboard.put('jobIdentity', {
                'visit': 85751839, 'snap': 0,
                'raft': "R:2,3", 'sensor': "S:1,1"
            })
        self.clipboard.put('inputDatasets', [
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="00"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="01"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="02"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="03"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="04"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="05"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="06"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="07"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="10"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="11"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="12"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="13"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="14"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="15"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="16"),
                Dataset("postIsr", visit=85751839, snap=0,
                    raft="R:2,3", sensor="S:1,1", channel="17")
            ])
    def tearDown(self):
        for key in self.__dict__.keys():
            del self.__dict__[key]
    def testPipe(self):
        """Pipeline test case."""
        ps = dafBase.PropertySet() 
        ps.set("input", self.datadir);
        dafPersist.LogicalLocation.setLocationMap(ps)
        p = pexPolicy.Policy()
        p0 = pexPolicy.Policy.createPolicy("CaInputStage.paf")
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
    else:        
        suites += unittest.makeSuite(IsrCcdAssemblyTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
