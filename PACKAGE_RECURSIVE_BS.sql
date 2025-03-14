create or replace PACKAGE BODY PACKAGE_RECURSIVE_BS IS

  FUNCTION ALIM_DONNEES_P1(NOMLOG      VARCHAR2,
                                  P_DATE_TRAI DATE,
                                  V_INSERTS  OUT NUMBER,
                                  V_UPDATES  OUT NUMBER,
                                  V_ERROR     OUT VARCHAR2) RETURN NUMBER IS

    V_ERR                NUMBER := 0;
    N_INS1               NUMBER := 0;
    N_UPD1               NUMBER := 0;
    N_INS2               NUMBER := 0;
    N_UPD2               NUMBER := 0;
    FILE_ID              UTL_FILE.FILE_TYPE;
    RES                  NUMBER := 0;
    SQL_TRAIT            VARCHAR2(4000);
    V_ENT             VARCHAR2(6);
    V_MNT            VARCHAR2(25);--NUMBER(18,3);
    V_ANNEE              NUMBER(4);
    V_MOIS               NUMBER(2);
    V_ERREUR             VARCHAR2(20);

-------------------------------------------------------------------------------------------
        /*DECLARATION DU CURSEUR*/

    CURSOR CUR_HD_SG IS
      SELECT P1.SP1_ENT_1,
             P1.SP1_ENT_2,
             P1.SP1_ENT_3,
             P1.SP1_ENT_4,
             P1.SP1_ENT_5,
             P1.SP1_ENT_6,
             P1.SP1_ENT_7,
             P1.SP1_ENT_8,
             P1.SP1_ENT_9,
             P1.SP1_ENT_10
      FROM   SCHEMA1.TAB_P1 P1;
   

      CURSOR CUR_SG IS
      SELECT SP3_PERIODE,
             SP3_RUB,
             SP3_LIB_RUB,
             SP3_CLASSE,
             SP3_TYPE_PTF,
             SP3_CLE,
             SP3_VAL7,
             SP3_VAL_TOTAL,
             SP3_MNT_1,
             SP3_MNT_2,
             SP3_MNT_3,
             SP3_MNT_4,
             SP3_MNT_5,
             SP3_MNT_6,
             SP3_MNT_7,
             SP3_MNT_8,
             SP3_MNT_9,
             SP3_MNT_10
      FROM   SCHEMA1.TAB_P3
      WHERE  SP3_CLE IS NOT NULL;
         

      BEGIN
       FILE_ID := SCHEMA2.PACKAGE_PRINT.F_OPEN(NOMLOG);
       RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID, '', ' ## DEBUT P1 ##');
       RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                                   'ALIM_DONNEES_P1',
                                   '## CHARGEMENT TAB_BALASU ##');

        FOR REC_HD_SG IN CUR_HD_SG LOOP
            FOR i IN 1..20 LOOP
            FOR REC_SG IN CUR_SG LOOP
                    
                         SQL_TRAIT := 'select SP1_ENT_'||i ||' from SCHEMA1.TAB_P1';
                         EXECUTE IMMEDIATE SQL_TRAIT into V_ENT;
                       
                         
                         BEGIN
                         SQL_TRAIT := 'select to_number(replace(SP3_MNT_'||i ||', chr(13), '''')), to_number(substr(SP3_PERIODE, 1, 4)), to_number(substr(SP3_PERIODE, 6, 2)) from SCHEMA1.TAB_P3 SH3 WHERE SP3_CLE = ''' || REC_SG.SP3_CLE || '''';
                         
                         EXECUTE IMMEDIATE SQL_TRAIT into V_MNT, V_ANNEE, V_MOIS;
                         
                         EXCEPTION WHEN OTHERS THEN
                         --SQL_TRAIT := 'select SP3_MNT_'||i || ' from SCHEMA1.TAB_P3 SH3 WHERE SP3_CLE = ''' || REC_SG.SP3_CLE || '''';
                         --EXECUTE IMMEDIATE SQL_TRAIT into V_ERREUR;
                         RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                                         'ALIM_DONNEES_P1 ',
                                         'MESSAGE ERREUR PL/SQL : CLE : ' || REC_SG.SP3_CLE  || ' COMPTEUR : ' || i || ' VALEUR : ' || V_ERREUR || ' ' ||
                                         SQLERRM);
                         
                         RETURN 1;
                         END;
                         
                         IF (V_ENT is not null) then
                            BEGIN
                            INSERT INTO SCHEMA2.TAB_BALASU (
                                   BS_PY_ID,
                                   BS_AN_ID,
                                   BS_MS_ID,
                                   BS_RUB,
                                   BS_LIB_RUB,
                                   BS_CLASSE,
                                   BS_TYPE_PTF,
                                   BS_CLE,
                                   BS_ENTITY,
                                   BS_MNT_SG,
                                   DTCREA_OCC,
                                   NM_TRT_OCC
                              )
                              VALUES (
                                   'F',
                                   V_ANNEE,
                                   V_MOIS,
                                   REC_SG.SP3_RUB,
                                   REC_SG.SP3_LIB_RUB,
                                   REC_SG.SP3_CLASSE,
                                   REC_SG.SP3_TYPE_PTF,
                                   REC_SG.SP3_CLE,
                                   V_ENT,
                                   V_MNT,
                                   SYSDATE,
                                   'ALIM_DONNEES_P1'
                             );
                             N_INS1 := N_INS1 +1;
                             EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
                             
                             UPDATE SCHEMA2.TAB_BALASU
                             SET    BS_RUB          = REC_SG.SP3_RUB,
                                    BS_LIB_RUB      = REC_SG.SP3_LIB_RUB,
                                    BS_CLASSE       = REC_SG.SP3_CLASSE,
                                    BS_TYPE_PTF     = REC_SG.SP3_TYPE_PTF,
                                    BS_MNT_SG       = V_MNT,
                                    DTMAJ_OCC       = SYSDATE
                             WHERE  BS_PY_ID        = 'F'
                             AND    BS_AN_ID        = V_ANNEE
                             AND    BS_MS_ID        = V_MOIS
                             AND    BS_CLE          = REC_SG.SP3_CLE
                             AND    BS_ENTITY       =  V_ENT;
                             N_UPD1 := N_UPD1+1;
                             END;
                                   
                         ELSE
                         exit;
                         END IF;
                         
                    END LOOP;
            END LOOP;
        END LOOP;
        
    COMMIT;
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                               'ALIM_DONNEES_P1',
                               'NOMBRE DE CREATIONS :' || N_INS1);
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                               'ALIM_DONNEES_P1',
                               'NOMBRE DE MISES A JOUR :' || N_UPD1);
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID, '', '');
    UTL_FILE.FCLOSE(FILE_ID);
    V_INSERTS := N_INS1;
    V_UPDATES := N_UPD1;

         RETURN V_ERR;
  END ALIM_DONNEES_P1;



  FUNCTION ALIM_DONNEES_P2(NOMLOG      VARCHAR2,
                                  P_DATE_TRAI DATE,
                                  V_INSERTS  OUT NUMBER,
                                  V_UPDATES  OUT NUMBER,
                                  V_ERROR     OUT VARCHAR2) RETURN NUMBER IS

    V_ERR                NUMBER := 0;
    N_INS1               NUMBER := 0;
    N_UPD1               NUMBER := 0;
    N_INS2               NUMBER := 0;
    N_UPD2               NUMBER := 0;
    FILE_ID              UTL_FILE.FILE_TYPE;
    RES                  NUMBER := 0;
    SQL_TRAIT            VARCHAR2(4000);
    V_ENT             VARCHAR2(6);
    V_MNT            VARCHAR2(25);--NUMBER(18,3);
    V_ANNEE              NUMBER(4);
    V_MOIS               NUMBER(2);
    V_ERREUR             VARCHAR2(20);

-------------------------------------------------------------------------------------------
        /*DECLARATION DU CURSEUR*/

    CURSOR CUR_HD_SG IS
      SELECT P1.SP1_ENT_1,
             P1.SP1_ENT_2,
             P1.SP1_ENT_3,
             P1.SP1_ENT_4,
             P1.SP1_ENT_5,
             P1.SP1_ENT_6,
             P1.SP1_ENT_7,
             P1.SP1_ENT_8,
             P1.SP1_ENT_9,
             P1.SP1_ENT_10
      FROM   SCHEMA1.TAB_P1 P1;
   

      CURSOR CUR_SG IS
      SELECT SP3_PERIODE,
             SP3_RUB,
             SP3_LIB_RUB,
             SP3_CLASSE,
             SP3_TYPE_PTF,
             SP3_CLE,
             SP3_VAL7,
             SP3_VAL_TOTAL,
             SP3_MNT_1,
             SP3_MNT_2,
             SP3_MNT_3,
             SP3_MNT_4,
             SP3_MNT_5,
             SP3_MNT_6,
             SP3_MNT_7,
             SP3_MNT_8,
             SP3_MNT_9,
             SP3_MNT_10
      FROM   SCHEMA1.TAB_P3
      WHERE  SP3_CLE IS NOT NULL;
         

      BEGIN
       FILE_ID := SCHEMA2.PACKAGE_PRINT.F_OPEN(NOMLOG);
       RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID, '', ' ## DEBUT P1 ##');
       RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                                   'ALIM_DONNEES_P2',
                                   '## CHARGEMENT TAB_BALASU ##');

        FOR REC_HD_SG IN CUR_HD_SG LOOP
            FOR i IN 1..20 LOOP
            FOR REC_SG IN CUR_SG LOOP
                    
                         SQL_TRAIT := 'select SP1_ENT_'||i ||' from SCHEMA1.TAB_P1';
                         EXECUTE IMMEDIATE SQL_TRAIT into V_ENT;
                       
                         
                         BEGIN
                         SQL_TRAIT := 'select to_number(replace(SP3_MNT_'||i ||', chr(13), '''')), to_number(substr(SP3_PERIODE, 1, 4)), to_number(substr(SP3_PERIODE, 6, 2)) from SCHEMA1.TAB_P3 SH3 WHERE SP3_CLE = ''' || REC_SG.SP3_CLE || '''';
                         
                         EXECUTE IMMEDIATE SQL_TRAIT into V_MNT, V_ANNEE, V_MOIS;
                         
                         EXCEPTION WHEN OTHERS THEN
                         --SQL_TRAIT := 'select SP3_MNT_'||i || ' from SCHEMA1.TAB_P3 SH3 WHERE SP3_CLE = ''' || REC_SG.SP3_CLE || '''';
                         --EXECUTE IMMEDIATE SQL_TRAIT into V_ERREUR;
                         RES     := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                                         'ALIM_DONNEES_P2 ',
                                         'MESSAGE ERREUR PL/SQL : CLE : ' || REC_SG.SP3_CLE  || ' COMPTEUR : ' || i || ' VALEUR : ' || V_ERREUR || ' ' ||
                                         SQLERRM);
                         
                         RETURN 1;
                         END;
                         
                         IF (V_ENT is not null) then
                            BEGIN
                            INSERT INTO SCHEMA2.TAB_BALASU (
                                   BS_PY_ID,
                                   BS_AN_ID,
                                   BS_MS_ID,
                                   BS_RUB,
                                   BS_LIB_RUB,
                                   BS_CLASSE,
                                   BS_TYPE_PTF,
                                   BS_CLE,
                                   BS_ENTITY,
                                   BS_MNT_SG,
                                   DTCREA_OCC,
                                   NM_TRT_OCC
                              )
                              VALUES (
                                   'F',
                                   V_ANNEE,
                                   V_MOIS,
                                   REC_SG.SP3_RUB,
                                   REC_SG.SP3_LIB_RUB,
                                   REC_SG.SP3_CLASSE,
                                   REC_SG.SP3_TYPE_PTF,
                                   REC_SG.SP3_CLE,
                                   V_ENT,
                                   V_MNT,
                                   SYSDATE,
                                   'ALIM_DONNEES_P2'
                             );
                             N_INS1 := N_INS1 +1;
                             EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
                             
                             UPDATE SCHEMA2.TAB_BALASU
                             SET    BS_RUB          = REC_SG.SP3_RUB,
                                    BS_LIB_RUB      = REC_SG.SP3_LIB_RUB,
                                    BS_CLASSE       = REC_SG.SP3_CLASSE,
                                    BS_TYPE_PTF     = REC_SG.SP3_TYPE_PTF,
                                    BS_MNT_SG       = V_MNT,
                                    DTMAJ_OCC       = SYSDATE
                             WHERE  BS_PY_ID        = 'F'
                             AND    BS_AN_ID        = V_ANNEE
                             AND    BS_MS_ID        = V_MOIS
                             AND    BS_CLE          = REC_SG.SP3_CLE
                             AND    BS_ENTITY       =  V_ENT;
                             N_UPD1 := N_UPD1+1;
                             END;
                                   
                         ELSE
                         exit;
                         END IF;
                         
                    END LOOP;
            END LOOP;
        END LOOP;
        
    COMMIT;
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                               'ALIM_DONNEES_P2',
                               'NOMBRE DE CREATIONS :' || N_INS1);
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID,
                               'ALIM_DONNEES_P2',
                               'NOMBRE DE MISES A JOUR :' || N_UPD1);
    RES := SCHEMA2.PACKAGE_PRINT.F_WRITE(FILE_ID, '', '');
    UTL_FILE.FCLOSE(FILE_ID);
    V_INSERTS := N_INS1;
    V_UPDATES := N_UPD1;

         RETURN V_ERR;
  END ALIM_DONNEES_P2;


FUNCTION MAIN_BS_ALL(NOMLOG VARCHAR2, P_DATE_TRAI DATE, P_CODE_PAYS VARCHAR2 , P_PATH VARCHAR2 , P_FILENAME5 VARCHAR2) RETURN NUMBER IS
    V_ERR     NUMBER := 0;
    V_RET     NUMBER := 0;
    V_TRT     VARCHAR2(100) := NULL;
    V_ERR_TRT NUMBER := 0;
    V_ERR_100 NUMBER:=0;
  	V_ERROR VARCHAR2(4000);
  	V_INSERTS NUMBER;
  	V_UPDATES NUMBER;
    V_DATE_TRAITEMENT DATE;
    V_DATE_SITU       DATE;
    V_NOM_CHAIN VARCHAR2(30) := 'qtt_0jbs' || P_CODE_PAYS;
    V_PY_ID     CHAR(1) := P_CODE_PAYS;
    FILE_ID              UTL_FILE.FILE_TYPE;
    RES                  NUMBER := 0;
    V_ANNEE              NUMBER;
    V_MOIS               NUMBER;

    BEGIN
        FILE_ID := SCHEMA2.PACKAGE_PRINT.F_OPEN(NOMLOG);

        BEGIN
        SELECT MAX(DATE_DEB) INTO V_DATE_TRAITEMENT
        FROM SCHEMA2.TAB_99
        WHERE NOM_CHAIN = V_NOM_CHAIN;
        EXCEPTION
        WHEN OTHERS THEN
            COMMIT;
            V_RET := 1;
            RETURN V_RET;
        END;

      -------------------------------------------------
      -- CALCUL DATE SITUATION POUR PACKAGE_07 ET PACKAGE_100
      -------------------------------------------------
      SELECT TRUNC(LAST_DAY(P_DATE_TRAI)) INTO V_DATE_SITU FROM DUAL;
      IF (P_CODE_PAYS = '1') THEN
      ----------------------
      -- UPDATE TABLE 100 DEBUT
      ----------------------
      V_ERR_100 := SCHEMA2.PACKAGE_100.F_UPDATE_TAB_100_D(V_NOM_CHAIN,  SYSDATE, V_MOIS, V_ANNEE);
      BEGIN

      V_TRT     := 'ALIM_DONNEES_P1';
      V_ERR     := ALIM_DONNEES_P1(NOMLOG, P_DATE_TRAI, V_INSERTS, V_UPDATES, V_ERROR);
      V_ERR_TRT := SCHEMA2.PACKAGE_07.ALIM_TAB_99(V_TRT, P_DATE_TRAI, 'U', V_ERR, V_ERROR, V_INSERTS, V_UPDATES, V_DATE_SITU);

      IF V_ERR = 1 THEN
          -------------------------------
          -- UPDATE TABLE 100 ERROR
          -------------------------------
          V_ERR_100 := SCHEMA2.PACKAGE_100.F_UPDATE_TAB_100_A(V_NOM_CHAIN,  SYSDATE, V_ERR, V_ERROR, V_MOIS, V_ANNEE);
          V_RET := 1;
          RETURN V_RET;
      END IF;
      END;
      
      END IF;
      
    --------------------------------------------------------------------------------------------------
      
      IF (P_CODE_PAYS = '2') THEN
      ----------------------
      -- UPDATE TABLE 100 DEBUT
      ----------------------
      V_ERR_100 := SCHEMA2.PACKAGE_100.F_UPDATE_TAB_100_D(V_NOM_CHAIN,  SYSDATE, V_MOIS, V_ANNEE);
      BEGIN

      V_TRT     := 'ALIM_DONNEES_P2';
      V_ERR     := ALIM_DONNEES_P2(NOMLOG, P_DATE_TRAI, V_INSERTS, V_UPDATES, V_ERROR);
      V_ERR_TRT := SCHEMA2.PACKAGE_07.ALIM_TAB_99(V_TRT, P_DATE_TRAI, 'U', V_ERR, V_ERROR, V_INSERTS, V_UPDATES, V_DATE_SITU);

      IF V_ERR = 1 THEN
          -------------------------------
          -- UPDATE TABLE 100 ERROR
          -------------------------------
          V_ERR_100 := SCHEMA2.PACKAGE_100.F_UPDATE_TAB_100_A(V_NOM_CHAIN,  SYSDATE, V_ERR, V_ERROR, V_MOIS, V_ANNEE);
          V_RET := 1;
          RETURN V_RET;
      END IF;
      END;
      
      END IF;
      
      

    RETURN V_RET;

  END MAIN_BS_ALL;



END PACKAGE_RECURSIVE_BS;