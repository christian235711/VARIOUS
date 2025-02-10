
  FUNCTION ALIM_KPI_1(NOMLOG      VARCHAR2,
                         P_DATE_TRAI DATE,
                         P_CODE_PAYS VARCHAR2,
                         P_ANNEE     NUMBER,
                         P_MOIS      NUMBER,
                         P_BE_ID NUMBER) RETURN NUMBER AS
BEGIN

  DECLARE

  V_ERR   NUMBER := 0;
  V_INS   NUMBER := 0;
  V_UPD   NUMBER := 0;
  V_INS2   NUMBER := 0;
  V_UPD2   NUMBER := 0;  
  FILE_ID UTL_FILE.FILE_TYPE;
  RES     NUMBER := 0;
  V_NOM VARCHAR2(255) := 'ALIM_KPI_1';
  V_TAB1 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1';
  V_TAB2 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1_DET';
  V_NO_LINES     NUMBER := 0;
  V_KEY VARCHAR2(4000);
  V_KEY_DET VARCHAR2(4000);
  V_HASH NUMBER := 0;  
  V_HASH_DET NUMBER := 0; 
  V_HASH_2 NUMBER := 0;  
  V_HASH_DET_2 NUMBER := 0;
   
  
  CURSOR CUR1 IS                                   
                  select  TRU_AN_ID AS AN_ID,
                          TRU_MS_ID AS MS_ID,
                          TRU_PY_ID AS PY_ID,
                          NVL(TRU_SD_ID,0) AS SD_ID,
                          TRU_BE_ID AS BE_ID,
                          CANA.ID AS ID_CTRL,
                          P_DATE_TRAI AS GEN_DATE,
                          TRU_TRUMENT_id||'|'||TRU_Scit_Number||'|'||TRU_TRAF_AMT||'|'||TRU_OUT_amount AS DETAIL,
                          CASE WHEN COUNT(*) OVER(PARTITION BY NVL(TRU_SD_ID,0)) > MAX_ERROR THEN 1 ELSE 0 END CHECK_CODE,
                          NULL AS NO_LINES,
                          --CASE WHEN COUNT(*) OVER(PARTITION BY NVL(TRU_SD_ID,0)) > MAX_ERROR THEN COUNT(*) OVER(PARTITION BY NVL(TRU_SD_ID,0)) ELSE 0 END AS NO_ERROR
                          COUNT(*) OVER(PARTITION BY NVL(TRU_SD_ID,0)) AS NO_ERROR
                  from   SCHEMA1.TAB_INTR , SCHEMA1.TAB_PARAM_CTRL  CANA
                  where  TRU_an_id = P_ANNEE
                  and    TRU_ms_id = P_MOIS
                  and    TRU_py_id = P_CODE_PAYS
                  and    TRU_BE_id = P_BE_ID
                  and    TRU_Scit_Number is not null
                  and    TRU_Scit_Number <> '00'
                  and    TRU_Scit_Number <> '0'
                  and    TRU_TRAF_AMT <> TRU_OUT_amount
                  AND    CANA.ID = '1';
  /*******************************************************************************/
  BEGIN

  FILE_ID := FDSQAUT.F_OPEN(NOMLOG);
  RES := FDSQAUT.F_WRITE(FILE_ID, '', V_NOM ||'## Alimentation de la table '||V_TAB1||' ##'); 
  


  
  FOR REC_TRU_UPD IN CUR1    
  LOOP

        /*************************************************************************/
        BEGIN

        IF V_ERR=1 THEN EXIT;
        END IF;
        --V_MAJ1 :=0;
        IF MOD(V_UPD + V_INS, 100)=0 THEN COMMIT;
        END IF;

        --------------------------------------------------------------------------------------------------------
        -- 0/ DEBUT CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------   
        V_KEY :=                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                V_NO_LINES||
                                REC_TRU_UPD.CHECK_CODE||
                                REC_TRU_UPD.NO_ERROR;        
        
        V_KEY_DET :=
                                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                REC_TRU_UPD.DETAIL;  
                                
                                

        select ora_hash(V_KEY) INTO V_HASH from dual;
        select ora_hash(V_KEY_DET) INTO V_HASH_DET from dual;
        
        
        
        BEGIN 
             SELECT SCORE_HASH INTO V_HASH_2 FROM SCHEMA1.TAB_CTRL_1 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_2 := 0;
        END;
        
        BEGIN
             SELECT SCORE_HASH INTO V_HASH_DET_2 FROM SCHEMA1.TAB_CTRL_1_DET 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_DET_2 := 0;        
        END;
        
        --------------------------------------------------------------------------------------------------------
        -- 0/ FIN CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------  
        
      --------------------------------------------------------------------------------------------------------
        -- I/ DEBUT D INSERTION DE LA TABLE DE CONTROLE SCHEMA1.TAB_CTRL_1 (1 INSERTION SUFFIT PAR CONTROLE)
      -------------------------------------------------------------------------------------------------------
                       
        BEGIN 
            SELECT COUNT(*) INTO V_NO_LINES 
             from   SCHEMA1.TAB_INTR , SCHEMA1.TAB_PARAM_CTRL  CANA
             where  TRU_an_id = P_ANNEE
             and    TRU_ms_id = P_MOIS
             and    TRU_py_id = P_CODE_PAYS
             and    TRU_BE_id = P_BE_ID
             and    TRU_Scit_Number is not null
             and    NVL(TRU_SD_ID,0) = REC_TRU_UPD.SD_ID
             and    TRU_Scit_Number <> '00'
             and    TRU_Scit_Number <> '0'
             --and    TRU_TRAF_AMT <> TRU_OUT_amount
             AND    CANA.ID = '1';
        EXCEPTION
             WHEN OTHERS THEN
             V_NO_LINES := 0;
        END;
        
        
                       BEGIN
                                        
                                        INSERT INTO SCHEMA1.TAB_CTRL_1 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                NO_LINES,
                                CHECK_CODE,
                                NO_ERROR,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                V_NO_LINES,
                                REC_TRU_UPD.CHECK_CODE,
                                REC_TRU_UPD.NO_ERROR,
                                V_HASH
                                            );
                                        
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS  := V_INS + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      IF V_HASH <> V_HASH_2 THEN
                                             UPDATE  SCHEMA1.TAB_CTRL_1 
                                             SET
                                                    NO_LINES = V_NO_LINES,
                                                    CHECK_CODE = REC_TRU_UPD.CHECK_CODE,
                                                    NO_ERROR = REC_TRU_UPD.NO_ERROR ,
                                                    DATE_MAJ = P_DATE_TRAI,
                                                    SCORE_HASH = V_HASH
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL;           
                                
                                          
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD := V_UPD + 1;
                                          
                                       END IF;
                
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
                         
                                             
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE DE CONTROLE SCHEMA1.TAB_CTRL_1 
                       -------------------------------------------------------------------------------------------------------          
                
                       --------------------------------------------------------------------------------------------------------
                       -- II/ DEBUT D INSERTION DE LA TABLE CONTROLE DETAILLE SCHEMA1.TAB_CTRL_1_DET SI CHECK_CODE KO(1)
                       -------------------------------------------------------------------------------------------------------
                       --IF REC_TRU_UPD.CHECK_CODE = 1 THEN 
                                              
                          BEGIN
                          
                               INSERT INTO SCHEMA1.TAB_CTRL_1_DET 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                DETAIL,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                REC_TRU_UPD.DETAIL,
                                V_HASH_DET
                                            );
                                
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS2  := V_INS2 + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      IF V_HASH_DET <> V_HASH_DET_2 THEN
                                             UPDATE  SCHEMA1.TAB_CTRL_1_DET 
                                             SET
                                                    DETAIL = REC_TRU_UPD.DETAIL,
                                                    DATE_MAJ = P_DATE_TRAI,
                                                    SCORE_HASH = V_HASH_DET
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL;           
                                
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD2 := V_UPD2 + 1;
                                      END IF;    
                
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
                       --END IF;   
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE CONTROLE DETAILLE TAB_CTRL_1_DET 
                       -------------------------------------------------------------------------------------------------------
                    commit;            
				
        EXCEPTION WHEN OTHERS THEN
        V_ERR := 1;
				RES := FDSQAUT.F_WRITE(FILE_ID, V_NOM,'Message Erreur pl/sql :'||SQLERRM,'E');
            
        RETURN V_ERR;
        END;
   END LOOP;

   COMMIT;

   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB1,'Nombre de mises a jour : '||TO_CHAR(V_UPD)||', d''insertions : '||TO_CHAR(V_INS));
   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB2,'Nombre de mises a jour : '||TO_CHAR(V_UPD2)||', d''insertions : '||TO_CHAR(V_INS2));
   UTL_FILE.FCLOSE(FILE_ID);
   RETURN V_ERR;
   End;
   
  END ALIM_KPI_1;




  
  FUNCTION ALIM_KPI_3(NOMLOG      VARCHAR2,
                         P_DATE_TRAI DATE,
                         P_CODE_PAYS VARCHAR2,
                         P_ANNEE     NUMBER,
                         P_MOIS      NUMBER,
                         P_BE_ID NUMBER) RETURN NUMBER AS
BEGIN

  DECLARE

  V_ERR   NUMBER := 0;
  V_INS   NUMBER := 0;
  V_UPD   NUMBER := 0;
  V_INS2   NUMBER := 0;
  V_UPD2   NUMBER := 0;  
  FILE_ID UTL_FILE.FILE_TYPE;
  RES     NUMBER := 0;
  V_NOM VARCHAR2(255) := 'ALIM_KPI_3';
  V_TAB1 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1';
  V_TAB2 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1_DET';
  V_NO_LINES     NUMBER := 0;
  V_KEY VARCHAR2(4000);
  V_KEY_DET VARCHAR2(4000);
  V_HASH NUMBER := 0;  
  V_HASH_DET NUMBER := 0; 
  V_HASH_2 NUMBER := 0;  
  V_HASH_DET_2 NUMBER := 0;
  
  
  CURSOR CUR1 IS                                   
                  WITH MAMUT AS (
                  select distinct ins.TRU_ent_BE_id, 
                  ins.TRU_ent_entity_id
                  from   SCHEMA1.TAB_INT_ENT INS
                  where  TRU_ent_py_id = P_CODE_PAYS
                  and    TRU_ent_an_id = P_ANNEE
                  and    TRU_ent_ms_id = P_MOIS
                  and    TRU_ent_BE_id = P_BE_ID
                  and    TRU_ent_ROLE = 'MAMUT'
                  ),
                  TRUMENT as (
                  select distinct TRU_ENTITY_ID,
                         TRU_BE_id AS BE_ID,
                         TRU_CONTR_DC,
                         TRU_AN_ID AS AN_ID,
                         TRU_MS_ID AS MS_ID,
                         TRU_PY_ID AS PY_ID,
                         99 AS SD_ID
                  from   SCHEMA1.TAB_INTR
                  where  TRU_an_id = P_ANNEE
                  and    TRU_ms_id = P_MOIS
                  and    TRU_py_id = P_CODE_PAYS
                  and    TRU_BE_id = P_BE_ID
                  ),
                  RIKETRI as (
                  select RSM_ENTITY_IDENTIFIER,
                         RSM_BE_IDENTIFIER,
                         RSM_CONTR_DC
                  from   SCHEMA1.TAB_RIK_METR
                  where  rsm_an_id = P_ANNEE
                  and    rsm_ms_id = P_MOIS
                  and    rsm_py_id = P_CODE_PAYS
                  and    RSM_BE_IDENTIFIER = P_BE_ID
                  )
                  select  AN_ID,
                          MS_ID,
                          PY_ID,
                          SD_ID,
                          BE_ID,
                          CANA.ID AS ID_CTRL,
                          P_DATE_TRAI AS GEN_DATE,
                          MAMUT.TRU_ent_entity_id ||'|'|| RIKETRI.RSM_ENTITY_IDENTIFIER  AS DETAIL,
                          CASE WHEN COUNT(*) OVER(PARTITION BY SD_ID) > MAX_ERROR THEN 1 ELSE 0 END CHECK_CODE,
                          NULL AS NO_LINES,
                          --CASE WHEN COUNT(*) OVER(PARTITION BY SD_ID) > MAX_ERROR THEN COUNT(*) OVER(PARTITION BY SD_ID) ELSE 0 END AS NO_ERROR
                          COUNT(*) OVER() AS NO_ERROR
                  from   MAMUT,
                         TRUMENT,
                         RIKETRI,
                         SCHEMA1.TAB_PARAM_CTRL cana
                  where  TRU_CONTR_DC is not null
                  and    RSM_CONTR_DC is not null
                  and    TRUMENT.BE_id = MAMUT.TRU_ent_BE_id
                  and    TRUMENT.TRU_ENTITY_ID = MAMUT.TRU_ent_entity_id
                  and    RIKETRI.RSM_ENTITY_IDENTIFIER = MAMUT.TRU_ent_entity_id
                  and    RIKETRI.RSM_BE_IDENTIFIER = MAMUT.TRU_ent_BE_id
                  AND    CANA.ID = '3';
  /*******************************************************************************/
  BEGIN

	FILE_ID := FDSQAUT.F_OPEN(NOMLOG);
  RES := FDSQAUT.F_WRITE(FILE_ID, '', V_NOM ||'## Alimentation de la table '||V_TAB1||' ##'); 
  

  
  FOR REC_TRU_UPD IN CUR1    
  LOOP

        /*************************************************************************/
        BEGIN
        
        IF V_ERR=1 THEN EXIT;
        END IF;
        --V_MAJ1 :=0;
        IF MOD(V_UPD + V_INS, 100)=0 THEN COMMIT;
        END IF;


        --------------------------------------------------------------------------------------------------------
        -- 0/ DEBUT CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------   
        V_KEY :=                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                V_NO_LINES||
                                REC_TRU_UPD.CHECK_CODE||
                                REC_TRU_UPD.NO_ERROR;        
        
        V_KEY_DET :=
                                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                REC_TRU_UPD.DETAIL;  
                                
                                

        select ora_hash(V_KEY) INTO V_HASH from dual;
        select ora_hash(V_KEY_DET) INTO V_HASH_DET from dual;
        
        
        
        BEGIN 
             SELECT SCORE_HASH INTO V_HASH_2 FROM SCHEMA1.TAB_CTRL_1 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_2 := 0;
        END;
        
        BEGIN
             SELECT SCORE_HASH INTO V_HASH_DET_2 FROM SCHEMA1.TAB_CTRL_1_DET 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_DET_2 := 0;        
        END;
        
        --------------------------------------------------------------------------------------------------------
        -- 0/ FIN CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------           
        --------------------------------------------------------------------------------------------------------
        -- I/ DEBUT D INSERTION DE LA TABLE DE CONTROLE SCHEMA1.TAB_CTRL_1 (1 INSERTION SUFFIT PAR CONTROLE)
        -------------------------------------------------------------------------------------------------------                       
                          BEGIN 
                          WITH MAMUT AS (
                          select distinct ins.TRU_ent_BE_id, 
                          ins.TRU_ent_entity_id
                          from   SCHEMA1.TAB_INT_ENT INS
                          where  TRU_ent_py_id = P_CODE_PAYS
                          and    TRU_ent_an_id = P_ANNEE
                          and    TRU_ent_ms_id = P_MOIS
                          and    TRU_ent_BE_id = P_BE_ID
                          and    TRU_ent_ROLE = 'MAMUT'
                          ),
                          TRUMENT as (
                          select distinct TRU_ENTITY_ID,
                                 TRU_BE_id AS BE_ID,
                                 TRU_CONTR_DC,
                                 TRU_AN_ID AS AN_ID,
                                 TRU_MS_ID AS MS_ID,
                                 TRU_PY_ID AS PY_ID,
                                 NVL(TRU_SD_ID,0) AS SD_ID
                          from   SCHEMA1.TAB_INTR
                          where  TRU_an_id = P_ANNEE
                          and    TRU_ms_id = P_MOIS
                          and    TRU_py_id = P_CODE_PAYS
                          and    TRU_BE_id = P_BE_ID
                          ),
                          RIKETRI as (
                          select RSM_ENTITY_IDENTIFIER,
                                 RSM_BE_IDENTIFIER,
                                 RSM_CONTR_DC
                          from   SCHEMA1.TAB_RIK_METR
                          where  rsm_an_id = P_ANNEE
                          and    rsm_ms_id = P_MOIS
                          and    rsm_py_id = P_CODE_PAYS
                          and    RSM_BE_IDENTIFIER = P_BE_ID
                          )
                          select  COUNT(*)
                          INTO    V_NO_LINES
                          from   MAMUT,
                                 TRUMENT,
                                 RIKETRI,
                                 SCHEMA1.TAB_PARAM_CTRL cana
                          where 1 = 1 
                          --and TRU_CONTR_DC is not null
                          --and    RSM_CONTR_DC is not null
                          and    TRUMENT.BE_id = MAMUT.TRU_ent_BE_id
                          and    TRUMENT.TRU_ENTITY_ID = MAMUT.TRU_ent_entity_id
                          and    RIKETRI.RSM_ENTITY_IDENTIFIER = MAMUT.TRU_ent_entity_id
                          and    RIKETRI.RSM_BE_IDENTIFIER = MAMUT.TRU_ent_BE_id
                          and    NVL(SD_ID,0) = REC_TRU_UPD.SD_ID
                          AND    CANA.ID = '3';
                          EXCEPTION
                          WHEN OTHERS THEN
                          V_NO_LINES := 0;
                          END;
  
                            
                          BEGIN
                                        
                                        INSERT INTO SCHEMA1.TAB_CTRL_1 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                NO_LINES,
                                CHECK_CODE,
                                NO_ERROR,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                V_NO_LINES,
                                REC_TRU_UPD.CHECK_CODE,
                                REC_TRU_UPD.NO_ERROR,
                                V_HASH
                                            );
                                        
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS  := V_INS + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      
                                      IF V_HASH <> V_HASH_2 THEN 
                                             UPDATE  SCHEMA1.TAB_CTRL_1 
                                             SET
                                                    NO_LINES = V_NO_LINES,
                                                    CHECK_CODE = REC_TRU_UPD.CHECK_CODE,
                                                    NO_ERROR = REC_TRU_UPD.NO_ERROR ,
                                                    DATE_MAJ = P_DATE_TRAI,
                                                    SCORE_HASH = V_HASH
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL;           
                                
                                          
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD := V_UPD + 1;
                                  
                                    END IF;
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
                          
                                             
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE DE CONTROLE SCHEMA1.TAB_CTRL_1 
                       -------------------------------------------------------------------------------------------------------          
                
                       --------------------------------------------------------------------------------------------------------
                       -- II/ DEBUT D INSERTION DE LA TABLE CONTROLE DETAILLE SCHEMA1.TAB_CTRL_1_DET SI CHECK_CODE KO(1)
                       -------------------------------------------------------------------------------------------------------
                       --IF REC_TRU_UPD.CHECK_CODE = 1 THEN 
                                              
                          BEGIN
                          
                               INSERT INTO SCHEMA1.TAB_CTRL_1_DET 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                DETAIL,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                REC_TRU_UPD.DETAIL,
                                V_HASH_DET
                                            );
                                
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS2  := V_INS2 + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      
                                      IF V_HASH_DET <> V_HASH_DET_2 THEN 
                                      
                                             UPDATE  SCHEMA1.TAB_CTRL_1_DET 
                                             SET
                                                    DETAIL = REC_TRU_UPD.DETAIL,
                                                    DATE_MAJ = P_DATE_TRAI,
                                                    SCORE_HASH = V_HASH_DET
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL;           
                                
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD2 := V_UPD2 + 1;
                                          
                                      END IF;
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
                       --END IF;   
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE CONTROLE DETAILLE TAB_CTRL_1_DET 
                       -------------------------------------------------------------------------------------------------------
                    commit;            
				
        EXCEPTION WHEN OTHERS THEN
        V_ERR := 1;
				RES := FDSQAUT.F_WRITE(FILE_ID, V_NOM,'Message Erreur pl/sql :'||SQLERRM,'E');
            
        RETURN V_ERR;
        END;
   END LOOP;

   COMMIT;

   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB1,'Nombre de mises a jour : '||TO_CHAR(V_UPD)||', d''insertions : '||TO_CHAR(V_INS));
   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB2,'Nombre de mises a jour : '||TO_CHAR(V_UPD2)||', d''insertions : '||TO_CHAR(V_INS2));
   UTL_FILE.FCLOSE(FILE_ID);
   RETURN V_ERR;
   End;
   
  END ALIM_KPI_3;

  
  FUNCTION ALIM_KPI_23(NOMLOG      VARCHAR2,
                         P_DATE_TRAI DATE,
                         P_CODE_PAYS VARCHAR2,
                         P_ANNEE     NUMBER,
                         P_MOIS      NUMBER,
                         P_BE_ID NUMBER) RETURN NUMBER AS
BEGIN

  DECLARE

  V_ERR   NUMBER := 0;
  V_INS   NUMBER := 0;
  V_UPD   NUMBER := 0;
  V_INS2   NUMBER := 0;
  V_UPD2   NUMBER := 0;  
  FILE_ID UTL_FILE.FILE_TYPE;
  RES     NUMBER := 0;
  V_KEY VARCHAR2(4000);
  V_KEY_DET VARCHAR2(4000);
  V_HASH NUMBER := 0;  
  V_HASH_DET NUMBER := 0; 
  V_HASH_2 NUMBER := 0;  
  V_HASH_DET_2 NUMBER := 0;
  V_NOM VARCHAR2(255) := 'ALIM_KPI_23';
  V_TAB1 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1';
  V_TAB2 VARCHAR2(255) := 'SCHEMA1.TAB_CTRL_1_DET';
  --V_NO_LINES     NUMBER := 0;
  
  CURSOR CUR1 IS                                   
            with TRUMENT AS (select count(*) AS NB_LINES_TRUMENT
            from   SCHEMA1.TAB_INTR
            where  TRU_an_id = P_ANNEE
            and    TRU_ms_id = P_MOIS
            and    TRU_py_id = P_CODE_PAYS
            and    TRU_BE_id = P_BE_ID
            )
            , INSTRUM_ENTITY AS(
            select count(*) AS NB_LNTR_ENT
            from   SCHEMA1.TAB_INT_ENT
            where  TRU_ent_an_id = P_ANNEE
            and    TRU_ent_ms_id = P_MOIS
            and    TRU_ent_py_id = P_CODE_PAYS
            and    TRU_ent_BE_id = P_BE_ID
            )

                  select  P_ANNEE AS AN_ID,
                          P_MOIS AS MS_ID,
                          P_CODE_PAYS AS PY_ID,
                          99 AS SD_ID,
                          P_BE_ID AS BE_ID,
                          CANA.ID AS ID_CTRL,
                          P_DATE_TRAI AS GEN_DATE,
                          NB_LINES_TRUMENT ||'|'|| NB_LNTR_ENT  AS DETAIL,
                          CASE WHEN NB_LNTR_ENT <= 4*NB_LINES_TRUMENT  THEN 0 ELSE 1 END CHECK_CODE,
                          NB_LNTR_ENT AS NO_LINES,
                          --CASE WHEN NB_LNTR_ENT <= 4*NB_LINES_TRUMENT  THEN 0 ELSE ABS(NB_LNTR_ENT - 4*NB_LINES_TRUMENT) END  AS NO_ERROR
                          ABS(NB_LNTR_ENT - 4*NB_LINES_TRUMENT) AS NO_ERROR
                  from   TRUMENT, INSTRUM_ENTITY, SCHEMA1.TAB_PARAM_CTRL  CANA
                  where  CANA.ID = '23';
  /*******************************************************************************/
  BEGIN

	FILE_ID := FDSQAUT.F_OPEN(NOMLOG);
  RES := FDSQAUT.F_WRITE(FILE_ID, '', V_NOM ||'## Alimentation de la table '||V_TAB1||' ##'); 
  
  FOR REC_TRU_UPD IN CUR1    
  LOOP

        /*************************************************************************/
        BEGIN
      
        IF V_ERR=1 THEN EXIT;
        END IF;
        --V_MAJ1 :=0;
        IF MOD(V_UPD + V_INS, 100)=0 THEN COMMIT;
        END IF;

  
  
        --------------------------------------------------------------------------------------------------------
        -- 0/ DEBUT CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------   
        V_KEY :=                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                REC_TRU_UPD.NO_LINES||
                                REC_TRU_UPD.CHECK_CODE||
                                REC_TRU_UPD.NO_ERROR;        
        
        V_KEY_DET :=
                                REC_TRU_UPD.AN_ID||
                                REC_TRU_UPD.MS_ID||
                                REC_TRU_UPD.PY_ID||
                                NVL(REC_TRU_UPD.SD_ID,0)||
                                REC_TRU_UPD.BE_ID||
                                REC_TRU_UPD.GEN_DATE||
                                REC_TRU_UPD.ID_CTRL||
                                REC_TRU_UPD.DETAIL;  
                                
                                

        select ora_hash(V_KEY) INTO V_HASH from dual;
        select ora_hash(V_KEY_DET) INTO V_HASH_DET from dual;
        
        
        
        BEGIN 
             SELECT SCORE_HASH INTO V_HASH_2 FROM SCHEMA1.TAB_CTRL_1 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_2 := 0;
        END;
        
        BEGIN
             SELECT SCORE_HASH INTO V_HASH_DET_2 FROM SCHEMA1.TAB_CTRL_1_DET 
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL; 
        
        EXCEPTION WHEN OTHERS THEN 
             V_HASH_DET_2 := 0;        
        END;
        
        --------------------------------------------------------------------------------------------------------
        -- 0/ FIN CALCULE SCORE DE HASH
        -------------------------------------------------------------------------------------------------------      


                          
                          BEGIN
                                        
                                        INSERT INTO SCHEMA1.TAB_CTRL_1 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                NO_LINES,
                                CHECK_CODE,
                                NO_ERROR,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                REC_TRU_UPD.NO_LINES,
                                REC_TRU_UPD.CHECK_CODE,
                                REC_TRU_UPD.NO_ERROR,
                                V_HASH
                                            );
                                        
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS  := V_INS + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      IF V_HASH <> V_HASH_2 THEN
                                             UPDATE  SCHEMA1.TAB_CTRL_1 
                                             SET
                                                    NO_LINES = REC_TRU_UPD.NO_LINES,
                                                    CHECK_CODE = REC_TRU_UPD.CHECK_CODE,
                                                    NO_ERROR = REC_TRU_UPD.NO_ERROR ,
                                                    DATE_MAJ = P_DATE_TRAI,
                                                    SCORE_HASH = V_HASH
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL;           
                                
                                          
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB1,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD := V_UPD + 1;
                                           
                                       END IF;
                
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB1,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
           
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE DE CONTROLE SCHEMA1.TAB_CTRL_1 
                       -------------------------------------------------------------------------------------------------------          
                
                       --------------------------------------------------------------------------------------------------------
                       -- II/ DEBUT D INSERTION DE LA TABLE CONTROLE DETAILLE SCHEMA1.TAB_CTRL_1_DET SI CHECK_CODE KO(1)
                       -------------------------------------------------------------------------------------------------------
                       --IF REC_TRU_UPD.CHECK_CODE = 1 THEN 
                                              
                          BEGIN
                          
                               INSERT INTO SCHEMA1.TAB_CTRL_1_DET 
                                          (
                                AN_ID,
                                MS_ID,
                                PY_ID,
                                SD_ID,
                                BE_ID,
                                GEN_DATE,
                                ID_CTRL,
                                DETAIL,
                                SCORE_HASH
                                )
                                        VALUES
                                          (
                                REC_TRU_UPD.AN_ID,
                                REC_TRU_UPD.MS_ID,
                                REC_TRU_UPD.PY_ID,
                                NVL(REC_TRU_UPD.SD_ID,0),
                                REC_TRU_UPD.BE_ID,
                                REC_TRU_UPD.GEN_DATE,
                                REC_TRU_UPD.ID_CTRL,
                                REC_TRU_UPD.DETAIL,
                                V_HASH_DET
                                            );
                                
                                        --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                        V_INS2  := V_INS2 + 1;
                                      
                                      EXCEPTION
                                      WHEN DUP_VAL_ON_INDEX THEN
                                      IF V_HASH_DET <> V_HASH_DET_2 THEN
                                             UPDATE  SCHEMA1.TAB_CTRL_1_DET 
                                             SET
                                                    DETAIL = REC_TRU_UPD.DETAIL,
                                                    DATE_MAJ = P_DATE_TRAI  ,
                                                    SCORE_HASH = V_HASH_DET
                                             WHERE  AN_ID = REC_TRU_UPD.AN_ID
                                             AND    MS_ID = REC_TRU_UPD.MS_ID
                                             AND    PY_ID = REC_TRU_UPD.PY_ID
                                             AND    SD_ID = NVL(REC_TRU_UPD.SD_ID,0)
                                             AND    BE_ID = REC_TRU_UPD.BE_ID
                                             AND    GEN_DATE = REC_TRU_UPD.GEN_DATE
                                             AND    ID_CTRL = REC_TRU_UPD.ID_CTRL
                                             AND    DETAIL = REC_TRU_UPD.DETAIL;           
                                
                                          --res := SCHEMA1.FDSQAUT.F_WRITE(file_id,'UPD '||V_TAB2,'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' OK' );
                                          V_UPD2 := V_UPD2 + 1;
                                          END IF;
                
                                    WHEN OTHERS THEN 
                                    COMMIT;
                                    V_ERR   := 1;
                                    res     := SCHEMA1.FDSQAUT.F_WRITE(file_id,V_TAB2,'Message Erreur pl/sql : ' ||sqlerrm ||'CONTROLE ID : ' || REC_TRU_UPD.ID_CTRL ||' KO');
                          
                          END;
                       --END IF;   
                       --------------------------------------------------------------------------------------------------------
                       -- FIN D INSERTION DE LA TABLE CONTROLE DETAILLE TAB_CTRL_1_DET 
                       -------------------------------------------------------------------------------------------------------
                    commit;            
				
        EXCEPTION WHEN OTHERS THEN
        V_ERR := 1;
				RES := FDSQAUT.F_WRITE(FILE_ID, V_NOM,'Message Erreur pl/sql :'||SQLERRM,'E');
            
        RETURN V_ERR;
        END;
   END LOOP;

   COMMIT;

   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB1,'Nombre de mises a jour : '||TO_CHAR(V_UPD)||', d''insertions : '||TO_CHAR(V_INS));
   RES := FDSQAUT.F_WRITE(FILE_ID, V_TAB2,'Nombre de mises a jour : '||TO_CHAR(V_UPD2)||', d''insertions : '||TO_CHAR(V_INS2));
   UTL_FILE.FCLOSE(FILE_ID);
   RETURN V_ERR;
   End;
   
  END ALIM_KPI_23;


FUNCTION MAIN_ALL(NOMLOG      VARCHAR2,
                    P_DATE_TRAI DATE DEFAULT SYSDATE,
                    P_ANNEE     NUMBER,
                    P_MOIS      NUMBER,
                    P_CODE_PAYS VARCHAR2) RETURN NUMBER IS
               

    V_STATUT NUMBER := 0;
    V_RET    NUMBER := 0;
    RES     NUMBER := 0;

    V_TRT             VARCHAR2(100) := NULL;
    V_ERR_TRT         NUMBER := 0;
    V_ERR             NUMBER := 0;
    V_ERROR           VARCHAR2(4000);
    V_INSERTS         NUMBER;
    V_UPDATES         NUMBER;
    V_DATE_TRAITEMENT DATE;
    V_ANNEE           NUMBER;
    V_MOIS            NUMBER;
    FILE_ID           UTL_FILE.FILE_TYPE;

    ---- CURSEUR DES COMPANY POUR FAIRE LES FICHIERS PAR CODE 
    CURSOR CUR_COMPANY IS
           SELECT CM.COM_CODE_SG, COMP_RC_CODE, COM_ENT_ID FROM SCHEMA1.TAB_COMP CM
           WHERE  SUBSTR(CM.COMP_RC_CODE, 1 ,1) = P_CODE_PAYS;           
---
  BEGIN

    ---------------------------
    -- FAIRE APPEL A LA FONCTION F_CONTROLE DANS UNE NOUVELLE BOUCLE DE CUR_COMPANY
    ---------------------------

 FOR REC IN CUR_COMPANY LOOP

    V_ERR := ALIM_KPI_1(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_ANNEE, P_MOIS, REC.COM_ENT_ID); 
    IF V_ERR = 1 THEN
      V_RET := 1;
      RETURN V_RET;
    END IF;
    
    V_ERR := ALIM_KPI_3(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_ANNEE, P_MOIS, REC.COM_ENT_ID);
    IF V_ERR = 1 THEN
      V_RET := 1;
      RETURN V_RET;
    END IF;

    V_ERR := ALIM_KPI_23(NOMLOG, P_DATE_TRAI, P_CODE_PAYS, P_ANNEE, P_MOIS, REC.COM_ENT_ID);
    IF V_ERR = 1 THEN
      V_RET := 1;
      RETURN V_RET;
    END IF;    


    
 END LOOP;   
 
 
    --UTL_FILE.FCLOSE(file_id);

    RETURN V_RET;

  END MAIN_ALL;


