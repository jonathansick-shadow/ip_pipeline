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
except NameError:
    display = True

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


def foo():
    if len(sys.argv) == 3:
        ix = int(sys.argv[1])
        iy = int(sys.argv[2])
    else:
        ix = 0
        iy = 0
    afwDataDir = eups.productDir("afwdata")
    obsDir = eups.productDir('obs_lsstSim')
    isrDir = eups.productDir('ip_isr')
    pipelineDir = eups.productDir('ip_pipeline')
    lutpolicypath = os.path.join(isrDir, "pipeline")
    pipepolicypath = os.path.join(pipelineDir, "policy")
    dc3MetadataFile = 'dc3MetadataPolicy.paf'
    simDatatypeFile = 'simDataTypePolicy.paf'
    suffix = "Keyword"

    """Pipeline test case."""
    clipboard = pexClipboard.Clipboard()
    clipboard.put('jobIdentity', {
        'visit': 1, 'snap': 0,
        'raft': "R:2,3", 'sensor': "S:1,1", 'channel': "%i%i"%(ix, iy)
    })
    clipboard.put('inputDatasets', [
        Dataset("raw", visit=1, snap=0,
                raft="R:2,3", sensor="S:1,1", channel="%i%i"%(ix, iy))
    ])
    clipboard.put("fwhm", 5.)
    p0 = pexPolicy.Policy.createPolicy("MakeCalibInputStage.paf")
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
    clipboard.put(p3.get("inputKeys.overscanfittype"), "MEDIAN")
    s3 = ipPipe.IsrOverscanStage(p3)
    t3 = SimpleStageTester(s3)
    file = pexPolicy.DefaultPolicyFile("ip_pipeline",
                                       "IsrBiasStageDictionary.paf",
                                       "policy")
    p4 = pexPolicy.Policy.createPolicy(file)
    s4 = ipPipe.IsrBiasStage(p4)
    t4 = SimpleStageTester(s4)

    o0 = t0.runWorker(clipboard)
    o2 = t2.runWorker(o0)
    if display:
        ds9.mtv(o2.get(p2.get("outputKeys.saturationCorrectedExposure")), frame=0,
                title= "Sat")
    o2.put(p3.get("inputKeys.exposure"),
           o2.get(p2.get("outputKeys.saturationCorrectedExposure")))
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
    exposure = o4.get(p4.get("outputKeys.biasSubtractedExposure"))
    exposure.writeFits("dark%i%i.fits"%(ix, iy))
    if display:
        ds9.mtv(exposure, frame=5, title="Output")

if __name__ == "__main__":
    foo()
