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

import sys
import os
import math
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


class IsrPipelineTestCase(unittest.TestCase):
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
            'raft': "2,3", 'sensor': "1,1", 'channel': "0,0", 'filter': "r"
        })
        self.clipboard.put('inputDatasets', [
            Dataset("raw", visit=85751839, snap=0,
                    raft="2,3", sensor="1,1", channel="0,0", filter="r")
        ])

    def tearDown(self):
        for key in self.__dict__.keys():
            del self.__dict__[key]

    def testPipe(self):
        """Pipeline test case."""
        try:
            ps = dafBase.PropertySet()
            ps.set("input", self.root)
            ps.set("cinput", self.root)
            dafPersist.LogicalLocation.setLocationMap(ps)
            p0 = pexPolicy.Policy.createPolicy(eups.productDir("ip_pipeline")+"/tests/"+"IsrInputStage.paf")
            s0 = lsst.pex.harness.IOStage.InputStage(p0)
            t0 = SimpleStageTester(s0)

            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                               "IsrSaturationStageDictionary.paf",
                                               "policy")
            p2 = pexPolicy.Policy.createPolicy(file)
            s2 = ipPipe.IsrSaturationStage(p2)
            t2 = SimpleStageTester(s2)
            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                               "IsrOverscanStageDictionary.paf",
                                               "policy")
            p3 = pexPolicy.Policy.createPolicy(file)
            s3 = ipPipe.IsrOverscanStage(p3)
            t3 = SimpleStageTester(s3)
            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                               "IsrBiasStageDictionary.paf",
                                               "policy")
            p4 = pexPolicy.Policy.createPolicy(file)
            s4 = ipPipe.IsrBiasStage(p4)
            t4 = SimpleStageTester(s4)
            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                               "IsrDarkStageDictionary.paf",
                                               "policy")
            p5 = pexPolicy.Policy.createPolicy(file)
            s5 = ipPipe.IsrDarkStage(p5)
            t5 = SimpleStageTester(s5)

            file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                               "IsrFlatStageDictionary.paf",
                                               "policy")
            p6 = pexPolicy.Policy.createPolicy(file)
            s6 = ipPipe.IsrFlatStage(p6)
            t6 = SimpleStageTester(s6)

            o0 = t0.runWorker(self.clipboard)
            o2 = t2.runWorker(o0)
            if display:
                ds9.mtv(o2.get(p2.get("outputKeys.saturationMaskedExposure")), frame=0,
                        title= "Sat")
            o2.put(p3.get("inputKeys.exposure"),
                   o2.get(p2.get("outputKeys.saturationMaskedExposure")))
            o3 = t3.runWorker(o2)
            if display:
                ds9.mtv(o3.get(p3.get("outputKeys.overscanCorrectedExposure")), frame=1,
                        title="Overscan")
            o3.put(p4.get("inputKeys.exposure"),
                   o3.get(p3.get("outputKeys.overscanCorrectedExposure")))
            o4 = t4.runWorker(o3)
            if display:
                ds9.mtv(o4.get(p4.get("outputKeys.biasSubtractedExposure")), frame=3,
                        title="Bias")
            o4.put(p5.get("inputKeys.exposure"),
                   o4.get(p4.get("outputKeys.biasSubtractedExposure")))
            o5 = t5.runWorker(o4)
            if display:
                ds9.mtv(o5.get(p5.get("outputKeys.darkSubtractedExposure")), frame=4,
                        title="Dark")
            o5.put(p6.get("inputKeys.exposure"),
                   o5.get(p5.get("outputKeys.darkSubtractedExposure")))
            o6 = t6.runWorker(o5)
            exposure = o6.get(p6.get("outputKeys.flatCorrectedExposure"))
            if writeFile:
                exposure.writeFits("postIsr.fits")
            if display:
                ds9.mtv(exposure, frame=5, title="Output")
        except Exception, e:
            print e


def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    elif not eups.productDir("obs_lsstSim"):
        print >> sys.stderr, "obs_lsstSim not set up; skipping test"
    else:
        suites += unittest.makeSuite(IsrPipelineTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)


def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
