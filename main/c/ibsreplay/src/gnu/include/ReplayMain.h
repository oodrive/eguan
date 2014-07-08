/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
/**
 * @file ReplayMain.h
 * @brief IBS Execution Replay Main
 * @author j. caba
 */
#ifndef REPLAYMAIN_H_
#define REPLAYMAIN_H_
#include "Replay.h"

static std::vector<std::string> welcome = {
        "                                   .awg.                       ",
        "    Welcome to                     =QQD                        ",
        "    IBS execution                  ]QQF                        ",
        "                                   jQQ[                        ",
        "                                   mQQ(                        ",
        "      ____,    .__i.    .___i,    :QQW`    .__i_,   ___,  .__= ",
        "    _yQQQQQ;  jQQQQm,  ]mQQQQQc   )WQE    jmQQQQQa  QQQ[  ]QQW ",
        "    ]QQQQQW .yQQQQQQk  jQQQQQQQc  ]QQF   jQWQQQQQD .WQQ[  jQQf ",
        "    jQQWWWk jQQQWWQQW  WQQWWQQQk  mQQ[  ]QQQWWQQQf  WWQ[ .QQQ' ",
        "    WQQ(   .QWQ( :QQ# :QQW` )QQW .mQQ;  mQQD` dQQ[  $WQ[ )QQE  ",
        "   :QQQ.   ]QQf .jWQB )QQE  .QQW :QQW  <QQW` .QQQ'  jQQL dQQf  ",
        "   )QQE    dQQmymWQQ[ jQQf  .QQW ]QQE  jQQf  =QQW   jQQk_QQW`  ",
        "   ]QQf    dQQQWQQQ?  dQQ(  =QQW jWQf  jQQ[  ]WQk   ]QQkjQQF   ",
        "   dQQ[    dQQ@HV?^   QQQ'  dQQF jQQ(  jQQ[  jWQF   )WQQQQW'   ",
        "   QQQ'    dQQc   _  =QQQ,_wQQQ' mQQL, jQQL .mQQ[   .QQQQQF    ",
        "  =QQW     3QQQgmmW  ]QQQQQQQQF  3QQQk ]QQQgmQQQL    WQQQW'    ",
        "  )QQE     )WQQQQQm  jQQQQQQQE   ]QQQ[ =QQQQQQQQE    dQQQf     ",
        "  ]QWf      ]WQQQQB  mQQWWW@!    -9QW;  ?QQQQTQWE    jQQD`     ",
        "   --        -'!H!` .QQW~--        -     'H!^ --  . vQQQ(      ",
        "                    :QQB                         jQmQQQF       ",
        "                    ]QQk                        .QQQQQE`       ",
        "                    jQQ[                        =QQQWF`        ",
        "                    G'F`                         QR'~          " };

/**
 * @brief Export main "algorithm" for unit tests
 */
extern int ibsreplay_main(const int argc, const char* argv[]);

#endif /* REPLAYMAIN_H_ */
