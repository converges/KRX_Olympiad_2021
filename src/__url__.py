'''
Crawling data with URL & requests data
'''

#1. KRX
#__(1) Listed/Delisted Tickers
krx_ticker_request_url = {
    'listed': 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
    'delisted': 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
}
krx_ticker_request_data = {
    'listed': {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
        'mktId': 'ALL', # 'ALL', 'STK': kospi, 'KSQ': kosdaq, 'KNX': konex
        'share': '1', # unit of the number of shares: '1', '1,000': K, '1,000,000': M
        },
    'delisted': {
        'bld': 'dbms/MDC/STAT/issue/MDCSTAT23801',
        'mktId': 'ALL', # 'ALL', 'STK': kospi, 'KSQ': kosdaq, 'KNX': konex
        'tboxisuCd_finder_listdelisu0_0': '전체',
        'isuCd': 'ALL',
        'isuCd2': 'ALL',
        'codeNmisuCd_finder_listdelisu0_0': '',
        'param1isuCd_finder_listdelisu0_0': '',
        'strtDd': '20081020', # start date
        'endDd': '20211105', # end date
        'share': '1', # unit of the number of shares: '1', '1,000': K, '1,000,000': M
        'csvxls_isNo': 'true'
        }
}
krx_ticker_response_cols = {
    'listed': {
        'ISU_SRT_CD': 'ticker',
        'ISU_NM': 'kor_name',
        'LIST_DD': 'listed_date',
        'MKT_TP_NM': 'market',
        'KIND_STKCERT_TP_NM': 'type'
    },
    'delisted': {
        'ISU_CD': "ticker",
        'ISU_NM': "kor_name",
        'LIST_DD': 'listed_date',
        'MKT_NM': "market",
        'KIND_STKCERT_TP_NM': "type",
        'DELIST_DD': "delisted_date"
    }
}

#__ (2) Short Balance
krx_short_balance_request_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
krx_short_balance_request_data = {
    'bld': 'dbms/MDC/STAT/srt/MDCSTAT30101',
    'searchType': '1',
    'mktId': 'STK', # STK-kospi, KSQ-kosdaq
    'secugrpId': 'STMFRTSCIFDRFS',
    'secugrpId': 'SRSW',
    'secugrpId': 'BC',
    'inqCond': 'STMFRTSCIFDRFSSRSWBC',
    'trdDd': '20211029', # date
    'tboxisuCd_finder_srtisu1_1': '',
    'isuCd': '',
    'isuCd2': '',
    'codeNmisuCd_finder_srtisu1_1': '',
    'param1isuCd_finder_srtisu1_1': '',
    'strtDd': '20210929',
    'endDd': '20211029',
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false'
}
krx_short_balance_response_cols = {
    'ISU_SRT_CD': 'ticker',
    'CVSRTSELL_TRDVOL': 'shortTrdQty', 
    'UPTICKRULE_APPL_TRDVOL': 'shortTrdQty_uptickrule_in', 
    'UPTICKRULE_EXCPT_TRDVOL': 'shortTrdQty_uptickrule_out', 
    'ACC_TRDVOL': 'trdQty', 
    'TRDVOL_WT': 'shortTrdRatio', 
    'CVSRTSELL_TRDVAL': 'shortTrdAmt', 
    'UPTICKRULE_APPL_TRDVAL': 'shortTrdAmt_uptickrule_in', 
    'UPTICKRULE_EXCPT_TRDVAL': 'shortTrdAmt_uptickrule_out', 
    'ACC_TRDVAL': 'trdAmt', 
    'TRDVAL_WT': 'shortAmtRatio'
}

#__ (3) Kospi
krx_kospi_request_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
krx_kospi_request_data = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT00301',
    'tboxindIdx_finder_equidx0_1': '코스피',
    'indIdx': '1',
    'indIdx2': '001',
    'codeNmindIdx_finder_equidx0_1': '코스피',
    'param1indIdx_finder_equidx0_1':' ',
    'strtDd': '20081020', # start date
    'endDd': '20211105', # end date
    'share': '1', # unit of the number of shares: '1', '1,000': K, '1,000,000': M.
    'money': '1', # unit of money: '1', '1,000': K, '1,000,000': M, '1,000,000,000': B.
    'csvxls_isNo': 'false'
}
krx_kospi_response_cols = {
    'TRD_DD': 'date',
    'CLSPRC_IDX': 'price',
    'OPNPRC_IDX': 'open',
    'HGPRC_IDX': 'high',
    'LWPRC_IDX': 'low',
    'ACC_TRDVOL': 'trdQty',
    'ACC_TRDVAL': 'trdAmt',
    'MKTCAP': 'mktcap_L'
    }



#__ Basic Data for each stock From KRX (basic data: ticker, stock type, listed market, IPO date, ...)
# 2. KOFIA
#__ (1) lending-borrowing balance for each ticker.
kofia_lendingbalance_request_url = "http://freesis.kofia.or.kr/meta/getMetaDataList.do"
kofia_lendingbalance_request_json = {
    "dmSearch": {
        "tmpV40": "1",
        "tmpV41": "1",
        "tmpV1": "D",
        "tmpV45": "20210928", # 'tmpV45': date.
        "tmpV46":"",
        "tmpV74":"1,0,,1",
        "OBJ_NM":"STATSCU0100000130BO"
        }
    }
kofia_lendingbalance_response_cols = {
    "TMPV2": "ticker", 
    "TMPV3": "lent", 
    "TMPV4": "repayed",
    "TMPV5": "balanceQty", 
    "TMPV6": "balanceAmt"
    }