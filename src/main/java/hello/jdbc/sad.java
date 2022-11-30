package hello.jdbc;

public class sad {

       <select id="selectArchiveList" parameterType="java.util.HashMap" resultType="ArchivesPrismVO">
    SELECT SUBSTR(aa.RM,6,2) AS RM,
    aa.aCount,
    CASE
    WHEN cc.MAX_DATE IS NOT NULL THEN cc.MAX_DATE ELSE NULL END AS MAX_DATE,
    CASE WHEN cc.MAX_COUNT IS NOT NULL THEN cc.MAX_COUNT ELSE 0 END AS MAX_COUNT
    FROM

            (

                    SELECT TO_CHAR(b.dt, 'YYYY-MM') AS RM
               , NVL(SUM(a.aCount), 0) aCount
    FROM ( SELECT TO_CHAR(REG_DATE, 'YYYYMMDD') AS REGDATE
                          , COUNT(*) aCount
    FROM KMDB.TBL_ARCHIVE_COUNT
    WHERE TO_DATE(TO_CHAR(REG_DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') BETWEEN TO_DATE(#{startDate}, 'YYYY')
    AND LAST_DAY(TO_DATE( #{endDate}, 'YYYY'))
    AND UPCATE =  #{upCateChk}
    GROUP BY TO_CHAR(REG_DATE, 'YYYYMMDD')
                    ) a
                  , ( SELECT TO_DATE( #{startDate},'YYYY') + LEVEL - 1 AS dt
    FROM dual
    CONNECT BY LEVEL &lt;= (LAST_DAY(TO_DATE(#{endDate},'YYYY'))
            - TO_DATE( #{startDate},'YYYY')) +1
            ) b
    WHERE b.dt = a.REGDATE(+)
    GROUP BY TO_CHAR(b.dt, 'YYYY-MM')
    ORDER BY TO_CHAR(b.dt, 'YYYY-MM') ASC

      ) aa

    LEFT JOIN (

            SELECT CONCAT(cc.REG_DATE, cc.MAX_DATE) AS REGDATE,  cc.* FROM (
            SELECT q2.REG_DATE, MAX(q2.ACOUNT) AS MAX_COUNT FROM (
    SELECT
    SUBSTR(q1.REG_DATE, 0,7) AS REG_DATE,
    COUNT(*) AS ACOUNT
    FROM
            (
                    SELECT TO_CHAR(REG_DATE, 'YYYY-MMDD') AS REG_DATE
    FROM KMDB.TBL_ARCHIVE_COUNT
    WHERE TO_DATE(TO_CHAR(REG_DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') BETWEEN TO_DATE(#{startDate}, 'YYYY')
    AND LAST_DAY(TO_DATE(#{endDate}, 'YYYY'))
    AND UPCATE = #{upCateChk}
    GROUP BY REG_DATE
                         ) q1
    GROUP BY q1.REG_DATE
    ORDER BY q1.REG_DATE
                     ) q2
    GROUP BY q2.REG_DATE
    ORDER BY q2.REG_DATE
                     ) tt
    LEFT JOIN (
            SELECT
                    SUBSTR(q3.REG_DATE, 0,7) AS REG_DATE,
    SUBSTR(q3.REG_DATE, 8,2) AS MAX_DATE,
    COUNT(*) AS MAX_COUNT
    FROM
            (
                    SELECT TO_CHAR(REG_DATE, 'YYYY-MMDD') AS REG_DATE
    FROM KMDB.TBL_ARCHIVE_COUNT
    WHERE TO_DATE(TO_CHAR(REG_DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') BETWEEN TO_DATE(#{startDate}, 'YYYY')
    AND LAST_DAY(TO_DATE(#{endDate}, 'YYYY'))
    AND UPCATE = #{upCateChk}
    GROUP BY REG_DATE
                         ) q3
    GROUP BY q3.REG_DATE
    ORDER BY q3.REG_DATE
                     ) cc ON tt.REG_DATE = cc.REG_DATE
    AND tt.MAX_COUNT = cc.MAX_COUNT
      ) cc ON
    aa.RM = cc.REG_DATE
    WHERE aa.RM LIKE '%' || #{yearWhere} || '%'
    ORDER BY aa.RM
            </select>
}
