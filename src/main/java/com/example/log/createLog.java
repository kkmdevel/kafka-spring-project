package com.example.log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class createLog {

    private static final String URL = "jdbc:mysql://192.168.56.101:3306/k_pro";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private static final String INSERT_SQL = "INSERT INTO web_log (" +
            "ssn_id, vstor_id, infrd_vstor_uid, cntn_grp_id, cntn_id, vym_cd, vst_dt, vst_dtm, " +
            "vtmd_cd, vst_dtm_tscd, vst_dywk_cd, wkn_vst_yn, vst_wknbr_cd, vst_srno, lusr_id, " +
            "cust_id, nwvst_yn, vst_ipadr, cooki_actv_yn, adid_actv_yn, bwsr_nm, bwsr_vrsn_nm, " +
            "bwsr_lng_nm, acdvc_tpcl_nm, acdvc_mfc_nm, acdvc_prd_nm, acdvc_os_nm, acdvc_os_vrsn_nm, " +
            "acdvc_dprsl, acrgn_nm, st_page_yn, epgdm_nm, epght_nm, epgpt_nm, epgpar_nm, epgurl, " +
            "epg_nm, epgex_nm, epgtl_nm, page_dwll_tscd, cvrs_yn, fnevt_yn, bunc_yn) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(createLog::insertRandomData, 0, 1, TimeUnit.SECONDS);
    }

    private static void insertRandomData() {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {

            LocalDateTime randomDateTime = generateRandomDateTime();
            LocalDateTime adjustedDateTime = randomDateTime.plus(9, ChronoUnit.HOURS);
            String vymCd = "2024" + String.format("%02d", adjustedDateTime.getMonthValue());

            pstmt.setString(1, generateAlphaNumericString(1, 7));  // ssn_id
            pstmt.setString(2, generateRandomString(10));  // vstor_id
            pstmt.setString(3, generateRandomString(15));  // infrd_vstor_uid
            pstmt.setString(4, generateRandomString(2));  // cntn_grp_id
            pstmt.setString(5, generateRandomString(4));  // cntn_id
            pstmt.setString(6, vymCd);  // vym_cd
            pstmt.setDate(7, java.sql.Date.valueOf(adjustedDateTime.toLocalDate()));  // vst_dt
            pstmt.setTimestamp(8, Timestamp.valueOf(adjustedDateTime));  // vst_dtm
            pstmt.setString(9, generateRandomString(2));  // vtmd_cd
            pstmt.setLong(10, adjustedDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());  // vst_dtm_tscd
            pstmt.setString(11, generateRandomString(3));  // vst_dywk_cd
            pstmt.setString(12, String.valueOf(RANDOM.nextInt(2)));  // wkn_vst_yn
            pstmt.setString(13, generateRandomString(5));  // vst_wknbr_cd
            pstmt.setInt(14, RANDOM.nextInt(100));  // vst_srno
            pstmt.setString(15, generateAlphaNumericString(3, 4));  // lusr_id
            pstmt.setString(16, generateRandomString(10));  // cust_id
            pstmt.setString(17, String.valueOf(RANDOM.nextInt(2)));  // nwvst_yn
            pstmt.setString(18, "192.168.0." + RANDOM.nextInt(256));  // vst_ipadr
            pstmt.setString(19, String.valueOf(RANDOM.nextInt(2)));  // cooki_actv_yn
            pstmt.setString(20, String.valueOf(RANDOM.nextInt(2)));  // adid_actv_yn
            pstmt.setString(21, "Browser" + RANDOM.nextInt(10));  // bwsr_nm
            pstmt.setString(22, "88.0." + RANDOM.nextInt(100));  // bwsr_vrsn_nm
            pstmt.setString(23, generateRandomString(2));  // bwsr_lng_nm
            pstmt.setString(24, "Type" + RANDOM.nextInt(5));  // acdvc_tpcl_nm
            pstmt.setString(25, "MFC" + RANDOM.nextInt(5));  // acdvc_mfc_nm
            pstmt.setString(26, "PRD" + RANDOM.nextInt(5));  // acdvc_prd_nm
            pstmt.setString(27, "OS" + RANDOM.nextInt(5));  // acdvc_os_nm
            pstmt.setString(28, "OSV" + RANDOM.nextInt(5));  // acdvc_os_vrsn_nm
            pstmt.setString(29, "1080x1920");  // acdvc_dprsl
            pstmt.setString(30, "Country" + RANDOM.nextInt(30));  // acrgn_nm
            pstmt.setString(31, String.valueOf(RANDOM.nextInt(2)));  // st_page_yn
            pstmt.setString(32, generateRandomString(8));  // epgdm_nm
            pstmt.setString(33, generateRandomString(8));  // epght_nm
            pstmt.setString(34, "/path/" + RANDOM.nextInt(100));  // epgpt_nm
            pstmt.setString(35, "param=" + RANDOM.nextInt(100));  // epgpar_nm
            pstmt.setString(36, "http://example.com/" + RANDOM.nextInt(100));  // epgurl
            pstmt.setString(37, "page" + RANDOM.nextInt(10));  // epg_nm
            pstmt.setString(38, "html");  // epgex_nm
            pstmt.setString(39, "Title" + RANDOM.nextInt(100));  // epgtl_nm
            pstmt.setInt(40, RANDOM.nextInt(500));  // page_dwll_tscd
            pstmt.setString(41, String.valueOf(RANDOM.nextInt(2)));  // cvrs_yn
            pstmt.setString(42, String.valueOf(RANDOM.nextInt(2)));  // fnevt_yn
            pstmt.setString(43, String.valueOf(RANDOM.nextInt(2)));  // bunc_yn

            pstmt.executeUpdate();
            System.out.println("새로운 랜덤 데이터를 삽입했습니다.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return sb.toString();
    }

    private static String generateAlphaNumericString(int alphaLength, int numericLength) {
        String alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        String numbers = "0123456789";
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < alphaLength; i++) {
            sb.append(alphabets.charAt(RANDOM.nextInt(alphabets.length())));
        }
        for (int i = 0; i < numericLength; i++) {
            sb.append(numbers.charAt(RANDOM.nextInt(numbers.length())));
        }
        return sb.toString();
    }

    private static LocalDateTime generateRandomDateTime() {
        int year = 2024;
        int month = RANDOM.nextInt(6) + 1;
        int day;

        if (month == 6) {
            day = RANDOM.nextInt(25) + 1;  // 1일부터 25일까지
        } else if (month == 2) {
            day = RANDOM.nextInt(29) + 1;  // 1일부터 29일까지 (윤년)
        } else if (month == 4 || month == 6 || month == 9 || month == 11) {
            day = RANDOM.nextInt(30) + 1;  // 1일부터 30일까지
        } else {
            day = RANDOM.nextInt(31) + 1;  // 1일부터 31일까지
        }

        LocalDate randomDate = LocalDate.of(year, month, day);
        LocalTime randomTime = LocalTime.of(RANDOM.nextInt(24), RANDOM.nextInt(60), RANDOM.nextInt(60), RANDOM.nextInt(1000) * 1000000);

        return LocalDateTime.of(randomDate, randomTime);
    }
}
