create or replace package body             PACKAGE_BASSOVER is

  FUNCTION EXPOND_BAC(NOMLOG      VARCHAR2,
                        P_DATE_TRAI DATE,
                        P_CODE_PAYS VARCHAR2,
                        P_PATH      VARCHAR2,
                        P_FILENAME  VARCHAR2,
                        P_DT_SITU   DATE,
                        P_JJ_ID     NUMBER,
                        P_MS_ID     NUMBER,
                        P_AN_ID     NUMBER,
                        P_COMPTEUR  NUMBER) RETURN NUMBER IS

    V_ERR       NUMBER := 0;
    N_SAUV      NUMBER := 0;
    FILE_ID     UTL_FILE.FILE_TYPE;
    file_id_cvs UTL_FILE.FILE_TYPE;
    FILE_NAME   VARCHAR2(12);
    RES         NUMBER := 0;
    v_ligne     VARCHAR2(4000);


CURSOR CUR_CL IS

select DISTINCT
    ID_CLE AS CODE_CLT,
    CASE WHEN NIVEAU_BAENT=0 OR CHAMPS_NIVEAU <=42
                THEN '0' 
        WHEN TYPE_PMT = 1 AND CHAMPS_NIVEAU >42 AND CHAMPS_NIVEAU<=95
                THEN '1' 
        WHEN PROBA_NIV_BAENT>0
                THEN '9' 
        ELSE ' ' END AS CHAMPS_CAT_BAENT,
    CHAMPS_NIVEAU AS CHAMPS_NT,
    CASE WHEN ABT_NIVEAU_BAENT = 1 AND CHAMPS_RT < CHAMPS_PL 
                THEN '1'
        WHEN ABT_PROBA_NIV_BAENT = 1
                THEN '2'
        ELSE '0' END AS CHAMPS_A1, 
    CASE WHEN ABT_NIVEAU_BAENT = 1 AND CHAMPS_TR = 'O'
                THEN '1'
        ELSE '0' END AS CHAMPS_A2 
    from SCHEMA1.TAB_DEF_NDD
    WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
    AND NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
    AND ND_JJ_ID = P_JJ_ID
    AND ND_AN_ID = P_AN_ID
    AND ND_MS_ID = P_MS_ID
    AND ID_CLE IS NOT NULL
    AND CHAMP_AP='O';

  BEGIN



  FILE_NAME := P_FILENAME;
  FILE_ID := SCHEMA1.PACKAGE_PROPEN.F_OPEN(NOMLOG);
  file_id_cvs:= SCHEMA1.PACKAGE_PROPEN.F_OPEN_CVS(P_PATH,FILE_NAME);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES BA BASSOVER', 'NUM_COMPTEUR :' || P_COMPTEUR);

       v_ligne :=(lpad('H',1,' ') || lpad('FQ1',3,' ') || SUBSTR(P_AN_ID, 3, 2) || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || P_COMPTEUR || P_AN_ID || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || lpad(to_char(sysdate,'YYYYMMDD'),8,' ') || lpad(to_char(sysdate,'HH24MMSS'),6,' '));
           RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
           
  FOR REC_CUR_CL IN CUR_CL LOOP

      BEGIN
           IF V_ERR=1 THEN
              EXIT;
           END IF;
           
           
           
           
             v_ligne :=
             rpad(REC_CUR_CL.CODE_CLT,33,' ')||
             lpad(REC_CUR_CL.CHAMPS_CAT_BAENT, 1, ' ')  ||
             lpad(REC_CUR_CL.CHAMPS_NT  ,5,0) ||
             lpad(REC_CUR_CL.CHAMPS_A1  ,1,' ') ||
             lpad(REC_CUR_CL.CHAMPS_A2  ,1,' ')
             ;

             

  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

            N_SAUV := N_SAUV + 1;
			EXCEPTION
      WHEN OTHERS THEN
                COMMIT;
                V_ERR  := 1;
                RES     := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,
                                          'EXPOND_DONNEES ',
                                         'MESSAGE ERREUR PL/SQL : ' ||
                                         SQLERRM );
                                         RETURN V_ERR;
  END;
  END LOOP;
  v_ligne := lpad('T',1,' ') || lpad(N_SAUV,10,0);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
  
  UTL_FILE.FCLOSE(file_id_cvs);
  COMMIT;

   RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES BA BASSOVER', 'NOMBRE DE LIGNES ECRITES :' || N_SAUV);
                        UTL_FILE.FCLOSE(FILE_ID);
    RETURN V_ERR;


END EXPOND_BAC;



FUNCTION EXPOND_RTN(NOMLOG      VARCHAR2,
                        P_DATE_TRAI DATE,
                         P_CODE_PAYS VARCHAR2,
                        P_PATH      VARCHAR2,
                        P_FILENAME  VARCHAR2,
                        P_DT_SITU   DATE,
                        P_JJ_ID     NUMBER,
                        P_MS_ID     NUMBER,
                        P_AN_ID     NUMBER,
                        P_COMPTEUR  NUMBER) RETURN NUMBER IS

    V_ERR       NUMBER := 0;
    N_SAUV      NUMBER := 0;
    FILE_ID     UTL_FILE.FILE_TYPE;
    file_id_cvs UTL_FILE.FILE_TYPE;
    FILE_NAME   VARCHAR2(12);
    RES         NUMBER := 0;
    v_ligne     VARCHAR2(4000);


CURSOR CUR_CLT IS

select DISTINCT
ID_CLE AS CODE_CLT,
CASE WHEN NIVEAU_BAENT=0 OR CHAMPS_NIVEAU <=42 AND ND_IND_UTP_CLT like '%UTP4%'
        THEN '0'
    WHEN TYPE_PMT = 1 AND CHAMPS_NIVEAU >42 AND CHAMPS_NIVEAU<=95
        THEN '1'
    WHEN PROBA_NIV_BAENT>0
        THEN '9' 
    ELSE ' '
END AS CHAMPS_CAT_BAENT,
CHAMPS_NIVEAU AS CHAMPS_NT,
CASE WHEN ABT_NIVEAU_BAENT = 1 AND TYPE_PMT = 0 
        THEN '1'
    WHEN  ABT_PROBA_NIV_BAENT = 1
        THEN '2'
    ELSE '0'
END AS CHAMPS_A1, 
CASE WHEN ABT_NIVEAU_BAENT = 1 AND CHAMPS_TR = 'O'
THEN '1'
ELSE '0'
END AS CHAMPS_A2 
from SCHEMA1.TAB_DEF_NDD
WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
AND NVL(CHAMPS_XX, ' ') IN('AB','CD')
AND ND_JJ_ID = P_JJ_ID
AND ND_AN_ID = P_AN_ID
AND ND_MS_ID = P_MS_ID
AND ID_CLE IS NOT NULL
AND CHAMP_AP='O';

  BEGIN



  FILE_NAME := P_FILENAME;
  FILE_ID := SCHEMA1.PACKAGE_PROPEN.F_OPEN(NOMLOG);
  file_id_cvs:= SCHEMA1.PACKAGE_PROPEN.F_OPEN_CVS(P_PATH,FILE_NAME);
  
  
            v_ligne :=(lpad('H',1,' ') || lpad('FQ3',3,' ') || SUBSTR(P_AN_ID, 3, 2) || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || P_COMPTEUR || P_AN_ID || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || lpad(to_char(sysdate,'YYYYMMDD'),8,' ') || lpad(to_char(sysdate,'HH24MMSS'),6,' '));
           RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

  FOR REC_CUR_CLT IN CUR_CLT LOOP

      BEGIN
           IF V_ERR=1 THEN
              EXIT;
           END IF;
             v_ligne :=
             rpad(REC_CUR_CLT.CODE_CLT,33,' ')||
             lpad(REC_CUR_CLT.CHAMPS_CAT_BAENT,1,0)||
             lpad(REC_CUR_CLT.CHAMPS_NT,5,0) ||
             lpad(REC_CUR_CLT.CHAMPS_A1  ,1,' ') ||
             lpad(REC_CUR_CLT.CHAMPS_A2  ,1,' ') ;



  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

            N_SAUV := N_SAUV + 1;
			EXCEPTION
      WHEN OTHERS THEN
                COMMIT;
                V_ERR  := 1;
                RES     := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,
                                          'EXPOND_DONNEES ',
                                         'MESSAGE ERREUR PL/SQL : ' ||
                                         SQLERRM );
                                         RETURN V_ERR;
  END;
  END LOOP;
  v_ligne := lpad('T',1,' ') || lpad(N_SAUV,10,0);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
  
  UTL_FILE.FCLOSE(file_id_cvs);
  COMMIT;

   RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES BA RENT', 'NOMBRE DE LIGNES ECRITES :' || N_SAUV);
                        UTL_FILE.FCLOSE(FILE_ID);
    RETURN V_ERR;


END EXPOND_RTN;



FUNCTION EXPOND_CT_BAC(NOMLOG      VARCHAR2,
                        P_DATE_TRAI DATE,
                        P_CODE_PAYS VARCHAR2,
                        P_PATH      VARCHAR2,
                        P_FILENAME  VARCHAR2,
                        P_DT_SITU   DATE,
                        P_JJ_ID     NUMBER,
                        P_MS_ID     NUMBER,
                        P_AN_ID     NUMBER,
                        P_COMPTEUR  NUMBER) RETURN NUMBER IS

    V_ERR       NUMBER := 0;
    N_SAUV      NUMBER := 0;
    FILE_ID     UTL_FILE.FILE_TYPE;
    file_id_cvs UTL_FILE.FILE_TYPE;
    FILE_NAME   VARCHAR2(12);
    RES         NUMBER := 0;
    v_ligne     VARCHAR2(4000);


CURSOR CUR_BAK IS

select 
ID_CLE AS CODE_CLT,
CASE WHEN CHAMPS_SD = 9
        THEN substr(CHAMPS_NO,5,12)
    WHEN CHAMPS_SD = 15
        THEN substr(CHAMPS_NO,7,6)||' '||substr(CHAMPS_NO,14,2)
    ELSE CHAMPS_NO
END AS NO_CONTRAT, 
CASE WHEN CHAMPS_TM = 0 OR CHAMPS_CT <=42 and ND_IND_UTP like '%UTP4%'
        THEN 0 
    WHEN ND_TM_DEF_PAI = 1 AND CHAMPS_CT >42 and CHAMPS_CT <=95
        THEN 1 
    WHEN ND_TM_DEF_PAI = 1 AND CHAMPS_CT >95
        THEN 2 
    WHEN ND_C6 >0 
        THEN 9 
    ELSE CHAMPS_TM
END AS CHAMPS_TA, 
CHAMPS_CT AS CHAMPS_CT,
CASE WHEN CHAMPS_TM = 0
        THEN 0  
    WHEN CHAMPS_CT <=12
        THEN 1
    WHEN CHAMPS_CT >12 AND CHAMPS_CT<=120
        THEN 6 
    WHEN CHAMPS_CT >120
        THEN 7 
    ELSE CHAMPS_TM
END AS CHAMPS_CTR,
TO_CHAR(P_DATE_TRAI - CHAMPS_CT, 'YYYYMMDD') AS CHAMPS_PAS, 
CASE WHEN NVL(NVL(TP_STG,TP_STG_M_1), 'XX') = 'S3' THEN '1' ELSE '0' END AS CHAMPS_FG, 
CASE  WHEN CHAMPS_SD IN (6, 7)  AND CHAMPS_TF ='VAC' 
    THEN 'VAC'
    ELSE CHAMPS_TF END AS CHAMPS_TPF
from SCHEMA1.TAB_DEF_NDD
WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
AND NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
AND ND_JJ_ID = P_JJ_ID
AND ND_AN_ID = P_AN_ID
AND ND_MS_ID = P_MS_ID
AND ID_CLE IS NOT NULL;

  BEGIN



  FILE_NAME := P_FILENAME;
  FILE_ID := SCHEMA1.PACKAGE_PROPEN.F_OPEN(NOMLOG);
  file_id_cvs:= SCHEMA1.PACKAGE_PROPEN.F_OPEN_CVS(P_PATH,FILE_NAME);



 v_ligne :=(lpad('H',1,' ') || lpad('FQ2',3,' ') || SUBSTR(P_AN_ID, 3, 2) || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || P_COMPTEUR || P_AN_ID || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || lpad(to_char(sysdate,'YYYYMMDD'),8,' ') || lpad(to_char(sysdate,'HH24MMSS'),6,' '));
           RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

  FOR REC_BAK IN CUR_BAK LOOP

      BEGIN
           IF V_ERR=1 THEN
              EXIT;
           END IF;
             v_ligne :=
             rpad(REC_BAK.CODE_CLT,33,' ')||
             rpad(REC_BAK.NO_CONTRAT,11,' ')||
             lpad(REC_BAK.CHAMPS_TA,1,0)   ||
			       lpad(REC_BAK.CHAMPS_CT,5,0)	||
             lpad(REC_BAK.CHAMPS_CTR,1,0)  ||
			       lpad(REC_BAK.CHAMPS_PAS,8,' ')   ||
			       lpad(REC_BAK.CHAMPS_FG,1,0)	||
             lpad(REC_BAK.CHAMPS_TPF,3,' ') ;


  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

            N_SAUV := N_SAUV + 1;
			EXCEPTION
      WHEN OTHERS THEN
                COMMIT;
                V_ERR  := 1;
                RES     := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,
                                          'EXPOND_DONNEES ',
                                         'MESSAGE ERREUR PL/SQL : ' ||
                                         SQLERRM );
                                         RETURN V_ERR;
  END;
  END LOOP;
  v_ligne := lpad('T',1,' ') || lpad(N_SAUV,10,0);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
  
  UTL_FILE.FCLOSE(file_id_cvs);
  COMMIT;

   RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES CO BASSOVER', 'NBR DE LIGNES ECRITES :' || N_SAUV);
                        UTL_FILE.FCLOSE(FILE_ID);
    RETURN V_ERR;


END EXPOND_CT_BAC;



FUNCTION EXPOND_CT_RTN(NOMLOG      VARCHAR2,
                        P_DATE_TRAI DATE,
                        P_CODE_PAYS VARCHAR2,
                        P_PATH      VARCHAR2,
                        P_FILENAME  VARCHAR2,
                        P_DT_SITU   DATE,
                        P_JJ_ID     NUMBER,
                        P_MS_ID     NUMBER,
                        P_AN_ID     NUMBER,
                        P_COMPTEUR  NUMBER) RETURN NUMBER IS

    V_ERR       NUMBER := 0;
    N_SAUV      NUMBER := 0;
    FILE_ID     UTL_FILE.FILE_TYPE;
    file_id_cvs UTL_FILE.FILE_TYPE;
    FILE_NAME   VARCHAR2(12);
    RES         NUMBER := 0;
    v_ligne     VARCHAR2(4000);


CURSOR CUR_RENT IS

select 
ID_CLE AS CODE_CLT,
CASE WHEN CHAMPS_SD = 9
        THEN substr(CHAMPS_NO,5,12)
    WHEN CHAMPS_SD = 15
        THEN substr(CHAMPS_NO,7,6)||substr(CHAMPS_NO,14,2)
    WHEN CHAMPS_SD IN (6, 7)
        THEN substr(CHAMPS_NO,1,10) 
    ELSE CHAMPS_NO
END AS NO_CONTRAT,  
CASE WHEN CHAMPS_TM = 0 OR CHAMPS_CT <=42  
        THEN 0 
    WHEN ND_TM_DEF_PAI = 1 AND CHAMPS_CT >42 and CHAMPS_CT <=95
        THEN 1 
    WHEN ND_C6 >0 
        THEN 9 
    ELSE CHAMPS_TM
END AS CHAMPS_TA,
CHAMPS_CT AS CHAMPS_CT,
CASE WHEN CHAMPS_TM = 0
        THEN 0  
    WHEN CHAMPS_CT <=12
        THEN 1 
    WHEN CHAMPS_CT >120
        THEN 7 
    ELSE CHAMPS_TM
END AS CHAMPS_CTR,
TO_CHAR(P_DATE_TRAI - CHAMPS_CT, 'YYYYMMDD') AS CHAMPS_PAS, --R5
CASE WHEN NVL(NVL(TP_STG,TP_STG_M_1), 'XX') = 'S3' THEN '1' ELSE '0' END AS CHAMPS_FG, 
CASE WHEN CHAMPS_SD IN (6, 7)  AND CHAMPS_TF ='VAC' 
        THEN 'VAC'
    ELSE CHAMPS_TF END AS CHAMPS_TPF
from SCHEMA1.TAB_DEF_NDD
WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
AND NVL(CHAMPS_XX, ' ') IN('AB','CD')
AND ND_JJ_ID = P_JJ_ID
AND ND_AN_ID = P_AN_ID
AND ND_MS_ID = P_MS_ID
AND ID_CLE IS NOT NULL;

  BEGIN



  FILE_NAME := P_FILENAME;
  FILE_ID := SCHEMA1.PACKAGE_PROPEN.F_OPEN(NOMLOG);
  file_id_cvs:= SCHEMA1.PACKAGE_PROPEN.F_OPEN_CVS(P_PATH,FILE_NAME);



 v_ligne :=(lpad('H',1,' ') || lpad('FQ4',3,' ') || SUBSTR(P_AN_ID, 3, 2) || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || P_COMPTEUR || P_AN_ID || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || lpad(to_char(sysdate,'YYYYMMDD'),8,' ') || lpad(to_char(sysdate,'HH24MMSS'),6,' '));
           RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

  FOR REC_RENT IN CUR_RENT LOOP

      BEGIN
           IF V_ERR=1 THEN
              EXIT;
           END IF;
             v_ligne :=
             rpad(REC_RENT.CODE_CLT,33,' ')||
             rpad(REC_RENT.NO_CONTRAT,11,' ')||
             lpad(REC_RENT.CHAMPS_TA,1,0)   ||
			       lpad(REC_RENT.CHAMPS_CT,5,0)	||
             lpad(REC_RENT.CHAMPS_CTR,1,0)  ||
			       lpad(REC_RENT.CHAMPS_PAS,8,' ')   ||
			       lpad(REC_RENT.CHAMPS_FG,1,0)	||
             lpad(REC_RENT.CHAMPS_TPF,3,' ');



  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);

            N_SAUV := N_SAUV + 1;
			EXCEPTION
      WHEN OTHERS THEN
                COMMIT;
                V_ERR  := 1;
                RES     := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,
                                          'EXPOND_DONNEES ',
                                         'MESSAGE ERREUR PL/SQL : ' ||
                                         SQLERRM );
                                         RETURN V_ERR;
  END;
  END LOOP;
  v_ligne := lpad('T',1,' ') || lpad(N_SAUV,10,0);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
  
  UTL_FILE.FCLOSE(file_id_cvs);
  COMMIT;

   RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES CO RENT', 'NBR DE LIGNES ECRITES :' || N_SAUV);
                        UTL_FILE.FCLOSE(FILE_ID);
    RETURN V_ERR;


END EXPOND_CT_RTN;

FUNCTION EXPOND_SECRECOB(NOMLOG      VARCHAR2,
                        P_DATE_TRAI DATE,
                        P_CODE_PAYS VARCHAR2,
                        P_PATH      VARCHAR2,
                        P_FILENAME  VARCHAR2,
                        P_DT_SITU   DATE,
                        P_JJ_ID     NUMBER,
                        P_MS_ID     NUMBER,
                        P_AN_ID     NUMBER,
                        P_COMPTEUR  NUMBER) RETURN NUMBER IS

    V_ERR       NUMBER := 0;
    N_SAUV      NUMBER := 0;
    FILE_ID     UTL_FILE.FILE_TYPE;
    file_id_cvs UTL_FILE.FILE_TYPE;
    FILE_NAME   VARCHAR2(12);
    RES         NUMBER := 0;
    v_ligne     VARCHAR2(4000);
    V_AN_ID_J_1     NUMBER(4);
    V_MS_ID_J_1     NUMBER(2);
    V_JJ_ID_J_1     NUMBER(2);
    V_DATE_SITU_J_1 DATE;
    CHAMPS_ST_J_1 SCHEMA1.TAB_DEF_NDD.CHAMPS_ST%TYPE;
    V_CHAMPS_ST_J SCHEMA1.TAB_DEF_NDD.CHAMPS_ST%TYPE;
    V_PROP_1 VARCHAR2(1);
    V_PROP_2 VARCHAR2(4);
    V_PROP_3 VARCHAR2(8);
    V_PROP_4   VARCHAR2(20);
    V_NB_TR NUMBER(2);
    V_NB_TR_TRAITE NUMBER(2);
    V_ANBANCA   VARCHAR2(1);
    V_NB_TIERS  NUMBER(10);
    --V_CD_FIMO VARCHAR2(4);
    --V_DT_FIMO VARCHAR2(8);
    V_SIGNE VARCHAR2(255);

    V_PROP_A VARCHAR2(8);
    V_PROP_B VARCHAR2(8);
    V_CD_FIMO_A VARCHAR2(4);
    --V_DT_FIMO_A VARCHAR2(8);
    V_CD_FIMO_B VARCHAR2(4);
    V_DT_FIMO_B VARCHAR2(8);

    V_CASE VARCHAR2(6);
    
    V_FULL_DELTA VARCHAR2(10) := 'DELTA';
    V_DATE_J_1   DATE;

CURSOR CUR_RENT IS

select 
ID_CLE AS CODE_CLT,
CHAMPS_NF      AS CHAMPS_NF,
CHAMPS_ST      AS CHAMPS_ST,
REPLACE(P_AN_ID||LPAD(P_MS_ID, 2, '0')||LPAD(P_JJ_ID, 2, '0'), ' ') AS DATE_SITUATION,
DECODE(SUBSTR(CHAMPS_DET, 1, 3), 'RET', 'RETA', CHAMPS_DET) AS CHAMPS_DET_BB
from SCHEMA1.TAB_DEF_NDD
WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
AND ND_JJ_ID = P_JJ_ID
AND ND_AN_ID = P_AN_ID
AND ND_MS_ID = P_MS_ID
AND NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
AND ID_CLE IS NOT NULL
GROUP BY 
ID_CLE,
CHAMPS_NF,
CHAMPS_ST,
REPLACE(P_AN_ID||LPAD(P_MS_ID, 2, '0')||LPAD(P_JJ_ID, 2, '0'), ' '),
DECODE(SUBSTR(CHAMPS_DET, 1, 3), 'RET', 'RETA', CHAMPS_DET);


CURSOR CUR_TITRI (P_ID_CLE VARCHAR2) IS
select /*+ INDEX(RT INDEX_NDD_4) */
 DISTINCT  LPAD(CHAMPS_TR, 2, '0') AS CHAMPS_TR
from SCHEMA1.TAB_DEF_NDD RT
WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
AND ND_JJ_ID = P_JJ_ID
AND ND_AN_ID = P_AN_ID
AND ND_MS_ID = P_MS_ID
AND NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
AND CHAMPS_TR IS NOT NULL
AND ID_CLE IS NOT NULL
AND ID_CLE = P_ID_CLE;

  BEGIN


  FILE_NAME := P_FILENAME;
  FILE_ID := SCHEMA1.PACKAGE_PROPEN.F_OPEN(NOMLOG);
  file_id_cvs:= SCHEMA1.PACKAGE_PROPEN.F_OPEN_CVS(P_PATH,FILE_NAME);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES CO RENT', 'DEBUT :');


 v_ligne :=(lpad('H',1,' ') || lpad('FQ5',3,' ') || SUBSTR(P_AN_ID, 3, 2) || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || P_COMPTEUR || P_AN_ID || LPAD(P_MS_ID, 2, '0') || LPAD(P_JJ_ID, 2, '0') || lpad(to_char(sysdate,'YYYYMMDD'),8,' ') || lpad(to_char(sysdate,'HH24MMSS'),6,' '));
           RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
           
  SELECT TO_DATE(P_AN_ID||LPAD(P_MS_ID, 2, '0')||LPAD(P_JJ_ID, 2, '0'), 'YYYYMMDD') -1
  INTO   V_DATE_SITU_J_1
  FROM DUAL;
  
  V_JJ_ID_J_1 := TO_NUMBER(TO_CHAR(V_DATE_SITU_J_1, 'DD'));
  V_MS_ID_J_1 := TO_NUMBER(TO_CHAR(V_DATE_SITU_J_1, 'MM'));
  V_AN_ID_J_1 := TO_NUMBER(TO_CHAR(V_DATE_SITU_J_1, 'YYYY'));

  FOR REC_RENT IN CUR_RENT LOOP

      BEGIN
           IF V_ERR=1 THEN
              EXIT;
           END IF;
           
           V_CASE := 'AUCUN';
           
           
             BEGIN
                  SELECT /*+ INDEX(RT INDEX_NDD_4) */ DISTINCT CHAMPS_ST
                  INTO   CHAMPS_ST_J_1
                  FROM   SCHEMA1.TAB_DEF_NDD RT
                  WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                  AND    ND_JJ_ID = V_JJ_ID_J_1
                  AND    ND_AN_ID = V_AN_ID_J_1
                  AND    ND_MS_ID = V_MS_ID_J_1
                  AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
                  AND    ID_CLE = REC_RENT.CODE_CLT;
             EXCEPTION   WHEN OTHERS THEN
                  CHAMPS_ST_J_1 := NULL;
             END;
             
                   
             V_DATE_J_1 := TO_DATE(REPLACE(V_AN_ID_J_1||LPAD(V_MS_ID_J_1, 2, '0')||LPAD(V_JJ_ID_J_1, 2, '0'), ' '), 'YYYYMMDD');
             
             V_PROP_4 := '';
             V_NB_TR := 0;
             V_NB_TR_TRAITE := 0;
             
             BEGIN 
             SELECT /*+ INDEX(RT INDEX_NDD_4) */ COUNT(DISTINCT CHAMPS_TR ) INTO V_NB_TR
             FROM   SCHEMA1.TAB_DEF_NDD RT
             WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
             AND    ND_JJ_ID = P_JJ_ID
             AND    ND_AN_ID = P_AN_ID
             AND    ND_MS_ID = P_MS_ID
             AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
             AND    CHAMPS_TR IS NOT NULL
             AND    ID_CLE IS NOT NULL
             AND    ID_CLE = REC_RENT.CODE_CLT;
             EXCEPTION WHEN OTHERS THEN
             V_NB_TR := 0;
             END;
             
             
             IF V_NB_TR <> 0 THEN
             FOR REC IN CUR_TITRI(REC_RENT.CODE_CLT) LOOP
                 V_NB_TR_TRAITE := V_NB_TR_TRAITE +1;
                 IF V_NB_TR_TRAITE = V_NB_TR THEN 
                 V_PROP_4 := V_PROP_4 || REC.CHAMPS_TR;
                 ELSE
                 V_PROP_4 := V_PROP_4 || REC.CHAMPS_TR || ',';
                 END IF;
             END LOOP;
             END IF;
             
             
             BEGIN
             SELECT /*+ INDEX(RT INDEX_NDD_4) */ COUNT(*) INTO V_NB_TIERS
             FROM SCHEMA1.TAB_DEF_NDD RT
             WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
             AND ND_JJ_ID = P_JJ_ID
             AND ND_AN_ID = P_AN_ID
             AND ND_MS_ID = P_MS_ID
             AND NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
             AND CHAMPS_TR IS NOT NULL
             AND ID_CLE IS NOT NULL
             AND ID_CLE = REC_RENT.CODE_CLT
             AND ND_MT_EXPOSURE > 0;
             EXCEPTION WHEN OTHERS THEN
             V_NB_TIERS := 0;
             END;
             

            
             
             IF V_NB_TIERS > 0 THEN V_ANBANCA := '1'; ELSE V_ANBANCA := '0'; END IF;

             IF V_FULL_DELTA = 'DELTA' THEN
                -----------------MATRIX DELTA MODE  ---------------- 
 
                --CASE A: V_CD_FIMO_A 

                   
                --CASE B: V_CD_FIMO_B 
                BEGIN 
                   SELECT /*+ INDEX(RT INDEX_NDD_6) */ DISTINCT ND_CD_FIMO, TO_CHAR(ND_DT_CD_FIMO, 'YYYYMMDD')
                   INTO   V_CD_FIMO_B,
                          V_DT_FIMO_B
                   FROM   SCHEMA1.TAB_DEF_NDD RT
                   WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                   AND    ND_JJ_ID = P_JJ_ID
                   AND    ND_AN_ID = P_AN_ID
                   AND    ND_MS_ID = P_MS_ID
                   AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
                   --AND    ND_CD_FIMO IS NOT NULL
                   AND    ID_CLE IS NOT NULL
                   AND    ID_CLE = REC_RENT.CODE_CLT
                   --AND    CHAMPS_ST = 'SO'
                   AND    ND_MT_EXPOSURE > 0
                   AND    ND_CD_FIMO IS NOT NULL
                   AND    ND_DT_CD_FIMO >= V_DATE_J_1
                   AND NOT EXISTS (SELECT 1
                                      FROM   SCHEMA1.TAB_DEF_NDD RT2
                                       WHERE  RT2.ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                       AND    RT2.ND_JJ_ID = P_JJ_ID
                                       AND    RT2.ND_AN_ID = P_AN_ID
                                       AND    RT2.ND_MS_ID = P_MS_ID
                                       AND    NVL(RT2.CHAMPS_XX, ' ') NOT IN('AB','CD')
                                       AND    RT2.ID_CLE IS NOT NULL
                                       AND    RT2.ID_CLE = REC_RENT.CODE_CLT
                                       AND    RT2.ND_MT_EXPOSURE > 0
                                       AND    RT2.ND_CD_FIMO IS NULL
                                       --AND    ND_DT_CD_FIMO >= V_DATE_J_1
                                       ) ;
                   EXCEPTION WHEN NO_DATA_FOUND THEN
                   V_CD_FIMO_B := NULL;
                   V_DT_FIMO_B := NULL;
                   
                   WHEN OTHERS THEN
                             BEGIN
                                  WITH POPULATION AS (
                                  SELECT /*+ INDEX(RT INDEX_NDD_6) */ ND_CD_FIMO, ND_DT_CD_FIMO, RANK() OVER(PARTITION BY ID_CLE ORDER BY ND_DT_CD_FIMO DESC, ND_CD_FIMO ASC) RANK 
                                  FROM   SCHEMA1.TAB_DEF_NDD RT
                                  WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                  AND    ND_JJ_ID = P_JJ_ID
                                  AND    ND_AN_ID = P_AN_ID
                                  AND    ND_MS_ID = P_MS_ID
                                  AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
                                  --AND    ND_CD_FIMO IS NOT NULL
                                  AND    ID_CLE IS NOT NULL
                                  AND    ID_CLE = REC_RENT.CODE_CLT
                                  --AND    CHAMPS_ST = 'SO'
                                  AND    ND_MT_EXPOSURE > 0
                                  AND    ND_CD_FIMO IS NOT NULL
                                  AND    ND_DT_CD_FIMO >= V_DATE_J_1
                                  AND NOT EXISTS (SELECT 1
                                      FROM   SCHEMA1.TAB_DEF_NDD RT2
                                       WHERE  RT2.ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                       AND    RT2.ND_JJ_ID = P_JJ_ID
                                       AND    RT2.ND_AN_ID = P_AN_ID
                                       AND    RT2.ND_MS_ID = P_MS_ID
                                       AND    NVL(RT2.CHAMPS_XX, ' ') NOT IN('AB','CD')
                                       AND    RT2.ID_CLE IS NOT NULL
                                       AND    RT2.ID_CLE = REC_RENT.CODE_CLT
                                       AND    RT2.ND_MT_EXPOSURE > 0
                                       AND    RT2.ND_CD_FIMO IS NULL
                                       --AND    ND_DT_CD_FIMO >= V_DATE_J_1
                                       )
                                       )
                                  SELECT ND_CD_FIMO, TO_CHAR(ND_DT_CD_FIMO, 'YYYYMMDD')
                                  INTO   V_CD_FIMO_B,
                                         V_DT_FIMO_B
                                  FROM   POPULATION
                                  WHERE  RANK = 1;
                             EXCEPTION WHEN OTHERS THEN
                                       V_CD_FIMO_B := NULL;
                                       V_DT_FIMO_B := NULL;
                             END;
                   END;


             ELSE
                -----------------MATRIX FULL MODE  20240822---------------- 

                --CASE A: V_CD_FIMO_A 

                   
                --CASE B: V_CD_FIMO_B 
                BEGIN 
                   SELECT /*+ INDEX(RT INDEX_NDD_6) */ DISTINCT ND_CD_FIMO, TO_CHAR(ND_DT_CD_FIMO, 'YYYYMMDD')
                   INTO   V_CD_FIMO_B,
                          V_DT_FIMO_B
                   FROM   SCHEMA1.TAB_DEF_NDD RT
                   WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                   AND    ND_JJ_ID = P_JJ_ID
                   AND    ND_AN_ID = P_AN_ID
                   AND    ND_MS_ID = P_MS_ID
                   AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
                   --AND    ND_CD_FIMO IS NOT NULL
                   AND    ID_CLE IS NOT NULL
                   AND    ID_CLE = REC_RENT.CODE_CLT
                   --AND    CHAMPS_ST = 'SO'
                   AND    ND_MT_EXPOSURE > 0
                   AND    ND_CD_FIMO IS NOT NULL 
                   AND NOT EXISTS (SELECT 1
                                      FROM   SCHEMA1.TAB_DEF_NDD RT2
                                       WHERE  RT2.ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                       AND    RT2.ND_JJ_ID = P_JJ_ID
                                       AND    RT2.ND_AN_ID = P_AN_ID
                                       AND    RT2.ND_MS_ID = P_MS_ID
                                       AND    NVL(RT2.CHAMPS_XX, ' ') NOT IN('AB','CD')
                                       AND    RT2.ID_CLE IS NOT NULL
                                       AND    RT2.ID_CLE = REC_RENT.CODE_CLT
                                       AND    RT2.ND_MT_EXPOSURE > 0
                                       AND    RT2.ND_CD_FIMO IS NULL
                                       )
                   ;
                   EXCEPTION WHEN NO_DATA_FOUND THEN
                   V_CD_FIMO_B := NULL;
                   V_DT_FIMO_B := NULL;
                   
                   WHEN OTHERS THEN
                             BEGIN
                                  WITH POPULATION AS (
                                  SELECT /*+ INDEX(RT INDEX_NDD_6) */ ND_CD_FIMO, ND_DT_CD_FIMO, RANK() OVER(PARTITION BY ID_CLE ORDER BY ND_DT_CD_FIMO DESC, ND_CD_FIMO ASC) RANK 
                                  FROM   SCHEMA1.TAB_DEF_NDD RT
                                  WHERE  ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                  AND    ND_JJ_ID = P_JJ_ID
                                  AND    ND_AN_ID = P_AN_ID
                                  AND    ND_MS_ID = P_MS_ID
                                  AND    NVL(CHAMPS_XX, ' ') NOT IN('AB','CD')
                                  --AND    ND_CD_FIMO IS NOT NULL
                                  AND    ID_CLE IS NOT NULL
                                  AND    ID_CLE = REC_RENT.CODE_CLT
                                  --AND    CHAMPS_ST = 'SO'
                                  AND    ND_MT_EXPOSURE > 0
                                  AND    ND_CD_FIMO IS NOT NULL 
                                  AND NOT EXISTS (SELECT 1
                                      FROM   SCHEMA1.TAB_DEF_NDD RT2
                                       WHERE  RT2.ND_PY_ID = DECODE(P_CODE_PAYS, 'P', 'J', P_CODE_PAYS)
                                       AND    RT2.ND_JJ_ID = P_JJ_ID
                                       AND    RT2.ND_AN_ID = P_AN_ID
                                       AND    RT2.ND_MS_ID = P_MS_ID
                                       AND    NVL(RT2.CHAMPS_XX, ' ') NOT IN('AB','CD')
                                       AND    RT2.ID_CLE IS NOT NULL
                                       AND    RT2.ID_CLE = REC_RENT.CODE_CLT
                                       AND    RT2.ND_MT_EXPOSURE > 0
                                       AND    RT2.ND_CD_FIMO IS NULL
                                       )
                                  )
                                  SELECT ND_CD_FIMO, TO_CHAR(ND_DT_CD_FIMO, 'YYYYMMDD')
                                  INTO   V_CD_FIMO_B,
                                         V_DT_FIMO_B
                                  FROM   POPULATION
                                  WHERE  RANK = 1;
                             EXCEPTION WHEN OTHERS THEN
                                       V_CD_FIMO_B := NULL;
                                       V_DT_FIMO_B := NULL;
                             END;
                   END;
            END IF;    
        ---- REINITIALISATION DES VARIABLE 
            V_PROP_A := NULL;
            V_PROP_B := NULL;
            V_PROP_2 := NULL;
            V_PROP_3 := NULL;
            V_PROP_1 := NULL;
            V_CASE      := NULL;
            
        IF V_FULL_DELTA = 'DELTA' THEN 
            --- NEW RULES
            --CASE 1 
            IF (CHAMPS_ST_J_1 = ' ' OR CHAMPS_ST_J_1 IS NULL) AND (REC_RENT.CHAMPS_ST = ' '  OR REC_RENT.CHAMPS_ST IS NULL) THEN
                V_PROP_A := NULL;
                V_PROP_B := NULL;
                V_PROP_2 := NULL;
                V_PROP_3 := NULL;
                V_PROP_1 := NULL;
                V_CASE := 'CASE1';
            --END IF;             
            --CASE 2 
            ELSIF (CHAMPS_ST_J_1 <> ' ' AND CHAMPS_ST_J_1 <> 'SO' AND CHAMPS_ST_J_1 IS NOT NULL) AND (REC_RENT.CHAMPS_ST <> ' ' OR REC_RENT.CHAMPS_ST IS NOT NULL) AND REC_RENT.CHAMPS_ST = CHAMPS_ST_J_1 THEN 
                V_PROP_A := NULL;
                V_PROP_B := NULL;
                V_PROP_2 := NULL;
                V_PROP_3 := NULL;
                V_PROP_1 := NULL;
                V_CASE := 'CASE2';
            --END IF;             
            --CASE 3
            ELSIF (CHAMPS_ST_J_1 = ' ' OR CHAMPS_ST_J_1 IS NULL) AND (REC_RENT.CHAMPS_ST <> ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '    ';
                V_PROP_3 := '        ';
                V_PROP_1 := '0';
                V_CASE := 'CASE3';
            --END IF;            
            --CASE 4
            ELSIF (CHAMPS_ST_J_1 <> ' ' AND CHAMPS_ST_J_1 <> 'SO' AND CHAMPS_ST_J_1 IS NOT NULL) AND (REC_RENT.CHAMPS_ST <> ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) AND REC_RENT.CHAMPS_ST <> CHAMPS_ST_J_1 THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '    ';
                V_PROP_3 := '        ';
                V_PROP_1 := '0';
                V_CASE := 'CASE4';
            --END IF;              
           --CASE 5A: V_CD_FIMO_A 

            
            --CASE 5B: V_CD_FIMO_B
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST = 'SO' OR REC_RENT.CHAMPS_ST = ' ' OR REC_RENT.CHAMPS_ST IS NULL) AND V_CD_FIMO_B IS NOT NULL THEN            
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := 'SO';
                V_PROP_2 := '1006';
                V_PROP_3 := V_DT_FIMO_B;
                V_PROP_1 := '1';
                V_CASE := 'CASE5B';
            --END IF;                 
             
            --CASE 5C: V_CD_FIMO_C
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST = ' ' OR REC_RENT.CHAMPS_ST IS NULL) AND V_CD_FIMO_B IS NULL THEN            
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := 'BO';
                V_PROP_2 := '1007';
                V_PROP_3 := REC_RENT.DATE_SITUATION;
                V_PROP_1 := '0';
                V_CASE := 'CASE5C';
            --END IF;                     
            --CASE 5D 
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST <> 'SO' AND REC_RENT.CHAMPS_ST = ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '1007';
                V_PROP_3 := REC_RENT.DATE_SITUATION;
                V_PROP_1 := 0;
                V_CASE := 'CASE5D';
            --END IF;              
            --CASE 5E 
              -- NE JAMAIS ENVOYE
              
            END IF ;
            
         ELSE
            
            --CASE 1 
            IF (CHAMPS_ST_J_1 = ' ' OR CHAMPS_ST_J_1 IS NULL) AND (REC_RENT.CHAMPS_ST = ' '  OR REC_RENT.CHAMPS_ST IS NULL) THEN
                V_PROP_A := NULL;
                V_PROP_B := NULL;
                V_PROP_2 := NULL;
                V_PROP_3 := NULL;
                V_PROP_1 := NULL;
                V_CASE := 'CASE1';
            --END IF; 
            --CASE 2 
            ELSIF (CHAMPS_ST_J_1 <> ' ' AND CHAMPS_ST_J_1 <> 'SO' AND CHAMPS_ST_J_1 IS NOT NULL) AND (REC_RENT.CHAMPS_ST = ' ' OR REC_RENT.CHAMPS_ST IS NULL) THEN 
                V_PROP_A := NULL;
                V_PROP_B := NULL;
                V_PROP_2 := NULL;
                V_PROP_3 := NULL;
                V_PROP_1 := NULL;
                V_CASE := 'CASE2';
            --END IF; 
            --CASE 3
            ELSIF (CHAMPS_ST_J_1 = ' ' OR CHAMPS_ST_J_1 IS NULL) AND (REC_RENT.CHAMPS_ST <> ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '    ';
                V_PROP_3 := '        ';
                V_PROP_1 := '0';
                V_CASE := 'CASE3';
            --END IF;
            --CASE 4
            ELSIF (CHAMPS_ST_J_1 <> ' ' AND CHAMPS_ST_J_1 <> 'SO' AND CHAMPS_ST_J_1 IS NOT NULL) AND (REC_RENT.CHAMPS_ST <> ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '    ';
                V_PROP_3 := '        ';
                V_PROP_1 := '0';
                V_CASE := 'CASE4';
            --END IF;                      
            
            --CASE 5A: V_CD_FIMO_A 
 
            
            --CASE 5B: V_CD_FIMO_B
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST = 'SO' OR REC_RENT.CHAMPS_ST = ' ' OR REC_RENT.CHAMPS_ST IS NULL) AND V_CD_FIMO_B IS NOT NULL THEN            
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := 'SO';
                V_PROP_2 := '1006';
                V_PROP_3 := V_DT_FIMO_B;
                V_PROP_1 := '1';
                V_CASE := 'CASE5B';
            --END IF;                 
             
            --CASE 5C: V_CD_FIMO_C
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST = ' ' OR REC_RENT.CHAMPS_ST IS NULL) AND V_CD_FIMO_B IS NULL THEN            
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := 'BO';
                V_PROP_2 := '1007';
                V_PROP_3 := REC_RENT.DATE_SITUATION;
                V_PROP_1 := '0';
                V_CASE := 'CASE5C';
            --END IF;                     
            --CASE 5D 
            ELSIF CHAMPS_ST_J_1 = 'SO' AND (REC_RENT.CHAMPS_ST <> 'SO' AND REC_RENT.CHAMPS_ST = ' ' AND REC_RENT.CHAMPS_ST IS NOT NULL) THEN
                V_PROP_A := CHAMPS_ST_J_1;
                V_PROP_B := REC_RENT.CHAMPS_ST;
                V_PROP_2 := '1007';
                V_PROP_3 := REC_RENT.DATE_SITUATION;
                V_PROP_1 := 0;
                V_CASE := 'CASE5D';
            END IF;              

         
         END IF;
          
          
          
          
          --RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,'', 'CODE_CLT:'||REC_RENT.CODE_CLT ||'--'|| 'V_CASE:' || V_CASE);   
           
            IF REC_RENT.CHAMPS_NF IS NOT NULL
            THEN V_SIGNE := '/';
            ELSE V_SIGNE := ' ';
            END IF;



             IF /*CHAMPS_ST_J_1 IS NOT NULL OR*/ V_PROP_B IS NOT NULL THEN  --V_CHAMPS_ST_J
             v_ligne :=
             rpad(REC_RENT.CODE_CLT,16,' ')||
             V_SIGNE ||
             rpad(NVL(REC_RENT.CHAMPS_NF, ' '),16,' ')||
             'TROM.10' ||
             lpad(nvl(CHAMPS_ST_J_1, ' '), 2, ' ') ||
             --lpad(nvl(REC_RENT.CHAMPS_ST, ' '), 2, ' ')   ||
             lpad(nvl(V_PROP_B, ' '), 2, ' ')   ||
             V_PROP_1 || 
             REC_RENT.DATE_SITUATION	||
             rpad(nvl(REC_RENT.CHAMPS_DET_BB, ' '), 4, ' ')  ||
			 V_PROP_2 || 
			 V_PROP_3 || 
             rpad(nvl(V_PROP_4, ' '), 10, ' ') ||
             V_ANBANCA || 
             lpad(' ', 20, ' ');
             
             RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
             N_SAUV := N_SAUV + 1;
            END IF;


            
			EXCEPTION
      WHEN OTHERS THEN
                COMMIT;
                V_ERR  := 1;
                RES     := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID,
                                          'EXPOND_DONNEES ',
                                         'MESSAGE ERREUR PL/SQL : ' ||
                                         SQLERRM || ' ' ||REC_RENT.CODE_CLT);
                                         RETURN V_ERR;
  END;
  END LOOP;
  v_ligne := lpad('T',1,' ') || lpad(N_SAUV,10,0);
  RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE_CVS(file_id_cvs, V_LIGNE);
  
  UTL_FILE.FCLOSE(file_id_cvs);
  COMMIT;

   RES := SCHEMA1.PACKAGE_PROPEN.F_WRITE(FILE_ID, 'EXPOND_DONNEES CO RENT', 'NBR DE LIGNES ECRITES :' || N_SAUV);
                        UTL_FILE.FCLOSE(FILE_ID);
    RETURN V_ERR;


END EXPOND_SECRECOB;



FUNCTION MAIN_SECRECOB(NOMLOG VARCHAR2, P_DATE_TRAI DATE,P_CODE_PAYS CHAR, P_PATH VARCHAR2, P_FILENAME1 VARCHAR2, P_FILENAME2 VARCHAR2, P_FILENAME3 VARCHAR2, P_FILENAME4 VARCHAR2, P_FILENAME5 VARCHAR2) RETURN NUMBER IS
    V_ERR     NUMBER := 0;
    V_RET     NUMBER := 0;
    V_TRT     VARCHAR2(100) := NULL;
    V_ERR_TRT NUMBER := 0;
    V_ERR_100 number:=0;
  	V_ERROR VARCHAR2(4000);
  	V_INSERTS NUMBER;
  	V_UPDATES NUMBER;
    V_PY_ID     CHAR(1) := P_CODE_PAYS;
    V_JJ_ID     NUMBER(2) := 0;
    V_MS_ID     NUMBER(2) := 0;
    V_AN_ID     NUMBER(4) := 0;
    V_DT_SITU   DATE;
    SQL_TEXT    VARCHAR2(4000);
    V_COMPTEUR  NUMBER(2) := 0;
 

    BEGIN
         
         ---------- DETERMINATION DE LA DATE DE SITUATION
         SQL_TEXT := '
  BEGIN
       SELECT DISTINCT TO_DATE( LPAD(RT'||P_CODE_PAYS||'_JJ_ID, 2, ''0'')|| LPAD(RT'||P_CODE_PAYS||'_MS_ID, 2, ''0'')|| RT'||P_CODE_PAYS||'_AN_ID, ''DDMMYYYY'')
       INTO   :V_DT_SITU
       FROM   SCHEMA2.TAB_TEMP_NDD_'||P_CODE_PAYS||';
  EXCEPTION WHEN OTHERS THEN
       :V_DT_SITU := NULL;
  END;';
  
  EXECUTE IMMEDIATE SQL_TEXT USING OUT V_DT_SITU;
  V_JJ_ID := TO_CHAR(V_DT_SITU, 'DD');
  V_MS_ID := TO_CHAR(V_DT_SITU, 'MM');
  V_AN_ID := TO_CHAR(V_DT_SITU, 'YYYY');
         
         ---------- RECUPERATION DU COMPTEUR
         BEGIN 
               SELECT NVL(MAX(NB_FLUX_CAB),0)+1 INTO V_COMPTEUR FROM TAB_DEF_APPAR WHERE date_succes = trunc(SYSDATE);
         EXCEPTION WHEN OTHERS THEN
               V_COMPTEUR := 1;
         END;
         
         BEGIN
              V_TRT     :='EXPOND_BAC';
              V_ERR     := EXPOND_BAC(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_PATH, P_FILENAME1, V_DT_SITU, V_JJ_ID, V_MS_ID, V_AN_ID, V_COMPTEUR);
              IF V_ERR = 1 THEN
                 V_RET := 1;
                 RETURN V_RET;
               END IF;
         END;
    
         BEGIN
              V_TRT     :='EXPOND_RTN';
              V_ERR     := EXPOND_RTN(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_PATH, P_FILENAME2, V_DT_SITU, V_JJ_ID, V_MS_ID, V_AN_ID, V_COMPTEUR);
              IF V_ERR = 1 THEN
                 V_RET := 1;
                 RETURN V_RET;
              END IF;
         END;
    
         BEGIN
              V_TRT     :='EXPOND_CT_BAC';
              V_ERR     := EXPOND_CT_BAC(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_PATH, P_FILENAME3, V_DT_SITU, V_JJ_ID, V_MS_ID, V_AN_ID, V_COMPTEUR);
              IF V_ERR = 1 THEN
                 V_RET := 1;
                 RETURN V_RET;
              END IF;
         END;
           
         BEGIN
              V_TRT     :='EXPOND_CT_RTN';
              V_ERR     := EXPOND_CT_RTN(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_PATH, P_FILENAME4, V_DT_SITU, V_JJ_ID, V_MS_ID, V_AN_ID, V_COMPTEUR);
              IF V_ERR = 1 THEN
                 V_RET := 1;
                 RETURN V_RET;
              END IF;
         END;
         
         BEGIN
              V_TRT     :='EXPOND_SECRECOB';
              V_ERR     := EXPOND_SECRECOB(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_PATH, P_FILENAME5, V_DT_SITU, V_JJ_ID, V_MS_ID, V_AN_ID, V_COMPTEUR);
              IF V_ERR = 1 THEN
                 V_RET := 1;
                 RETURN V_RET;
              END IF;
         END;
    
         BEGIN
              INSERT INTO TAB_DEF_APPAR (NB_FLUX_CAB, DATE_SUCCES)
              VALUES (V_COMPTEUR, trunc(SYSDATE));
         EXCEPTION WHEN OTHERS THEN 
              UPDATE TAB_DEF_APPAR cmp SET cmp.nb_flux_cab = V_COMPTEUR
              WHERE  DATE_SUCCES = trunc(SYSDATE);
         END;
         COMMIT;
    
         RETURN V_RET;
  END MAIN_SECRECOB;
  
END PACKAGE_BASSOVER;