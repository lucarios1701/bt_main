#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2020 Daniel Rodriguez
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from . import MovingAverageBase, Average


class MovingAverageSimple(MovingAverageBase):
    '''
    Non-weighted average of the last n periods

    Formula:
      - movav = Sum(data, period) / period

    See also:
      - http://en.wikipedia.org/wiki/Moving_average#Simple_moving_average
    '''
    alias = ('SMA', 'SimpleMovingAverage',)
    # @tuando: Need to have this lines to add to LineBuffer in Lines __init__
    lines = ('sma',)

    def __init__(self):
        # Before super to ensure mixins (right-hand side in subclassing)
        # can see the assignment operation and operate on the line
        # @tuando - guess: self.lines[0] have no data because this is 'sma' lines belonging to SMA class
        # but self.data has data because it is 'close' lines and belong to YahooCSVData class
        self.lines[0] = Average(self.data, period=self.p.period)
        # @tuando: note-guess: self.lines[0] is the LineBuffer 'sma' was set
        # by the LineAlias, set value into it will refer to __setitem__ of Line
        # then the set value will be refered to __set__ of LineAlias

        super(MovingAverageSimple, self).__init__()
