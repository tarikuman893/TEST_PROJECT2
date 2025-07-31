<?php

namespace App\Console\Commands\ImportMeasure;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportMeasureMp extends Command
{
    protected $signature   = 'import:measure-mp';
    protected $description = 'MP 計測 CSV を measure_mp へ取込む';

    public function handle(): int
    {
        $sourceDir  = storage_path('app/csvA8/Measure');
        $csvPattern = "$sourceDir/mp_measure.csv";

        // 日付文字列を null に変換
        $toDate = static function (?string $v): ?string {
            $v = trim($v ?? '');
            if ($v === '' || $v === '0000-00-00 00:00:00') {
                return null;
            }
            $ts = @strtotime($v);
            return $ts === false ? null : date('Y-m-d H:i:s', $ts);
        };

        // カンマを取り除いて整数化
        $toInt = static fn (?string $v): int => (int) str_replace(',', '', $v ?? '0');

        foreach (glob($csvPattern) as $csv) {
            // SJIS→UTF-8（もともと UTF-8 BOM の場合 auto で正しく読み込む）
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'auto')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()

                // --- 除外ロジック（元 CsvMeasureIDImportModel より）---
                ->reject(function(array $row) {
                    // 末尾の余分列を削除
                    array_pop($row);
                    // 空行・ヘッダー行 ("タイプ") をスキップ
                    if (empty($row) || strpos($row[0] ?? '', 'タイプ') !== false) {
                        return true;
                    }
                    // MOTA テスト CV 除外
                    if (($row[4] ?? '') === '2025/05/29 17:59:14') return true;
                    if (($row[4] ?? '') === '2025/05/28 15:11:56') return true;
                    // ミシュワン除外
                    if (($row[3] ?? '') === '2025/07/18 17:00:52') return true;
                    if (($row[3] ?? '') === '2025/07/18 10:24:42') return true;
                    if (($row[3] ?? '') === '2025/07/17 17:31:26') return true;
                    // その他指定日時除外
                    foreach ([
                        '2025-07-16 10:23:34','2025-07-15 18:46:10','2025-07-15 08:43:48',
                        '2025-07-15 08:03:19','2025-07-14 17:51:03','2025-07-13 10:24:20',
                        '2025-07-12 23:53:32','2025-07-11 15:03:20','2025-07-09 22:22:37',
                        '2025-07-09 08:14:30','2025-07-07 11:44:08','2025-07-05 20:54:31',
                        '2025-07-05 16:32:07'
                    ] as $dt) {
                        if (($row[4] ?? '') === $dt) return true;
                    }
                    return false;
                })

                ->each(function(array $row) use ($toDate, $toInt) {
                    // 余分列を削除
                    array_pop($row);

                    // --- MeasureID／TCLICKID の抽出 ---
                    $param = $row[8] ?? '';
                    // _YCLID_ → _CLID_ → _GCLID_ の順で分割
                    if (strpos($param, '_YCLID_') !== false) {
                        $parts = explode('_YCLID_', $param, 2);
                    } elseif (strpos($param, '_CLID_') !== false) {
                        $parts = explode('_CLID_', $param, 2);
                    } else {
                        $parts = explode('_GCLID_', $param, 2);
                    }
                    $measureId = null;
                    $tclickId  = null;
                    if (!empty($parts[0]) && strpos($parts[0], '_TCLICK_') !== false) {
                        [$measureId, $tclickId] = explode('_TCLICK_', $parts[0], 2);
                    } else {
                        $measureId = $parts[0] ?? null;
                    }

                    // --- gclid／utm_content の抽出 ---
                    $gclid = null;
                    $utmContent = null;
                    if (!empty($parts[1]) && strpos($parts[1], '_UTMC_') !== false) {
                        [$gclid, $utmContent] = explode('_UTMC_', $parts[1], 2);
                    } else {
                        $gclid = $parts[1] ?? null;
                    }

                    // --- MOTA 計測 ID 補完 ---
                    if (($row[1] ?? '') === '車査定/株式会社MOTA/MOTA車査定'
                        && ($row[12] ?? '') === 'MOTA_Meta'
                        && $measureId === null
                    ) {
                        $measureId = 'fb_usedcar-1_mota_mp';
                    }

                    // --- ミシュワンの計測ID是正（2025/07/01）---
                    if (($row[3] ?? '') === '2025/07/01 14:23:42') {
                        $measureId = 'gs_mishone_mp_TCLICK__CjwKZJs…';
                    }

                    // --- サイト名正規化 ---
                    // 葉酸サプリ
                    if (($row[12] ?? '') === 'YS_葉酸サプリ-2') {
                        $row[12] = '葉酸サプリ安心ランキング';
                    }
                    // ミシュワン通常化
                    if (in_array($row[12] ?? '', ['ミシュワン_消化LP','ミシュワン_アレルギーLP'], true)) {
                        $row[12] = 'ミシュワン_通常';
                    }
                    // ミシュワンリンク変更対応
                    if (strpos($param, 'gs_dogfood-1_mishone_mp') !== false
                        && ($row[12] ?? '') === 'ミシュワン_通常'
                    ) {
                        $row[12] = '転職エージェント評判｜-BEST WORK-';
                    }
                    // ドッグフード流入元別追記
                    if (strpos($param, 'ys_dogfood-1') !== false) {
                        $row[12] .= '（yahoo）';
                    }
                    if (strpos($param, 'ys_dogfood-2') !== false) {
                        $row[12] .= '（yahoo2）';
                    }
                    if (strpos($param, 'gs_dogfood-2') !== false) {
                        $row[12] .= '（google2）';
                    }
                    if (strpos($param, 'ms_dogfood-1') !== false) {
                        $row[12] = ($row[12] === 'ミシュワン_通常' ? 'ミシュワン_マイクロソフト' : $row[12]) . '（microsoft）';
                    }

                    // --- DB 登録 ---
                    DB::table('measure_mp')->insertOrIgnore([
                        'archive_type'        => $row[0]  ?? null,
                        'pg_name'             => $row[1]  ?? null,
                        'reward'              => $toInt($row[2]  ?? null),
                        'click_time'          => $toDate($row[3]  ?? null),
                        'occur_time'          => $toDate($row[4]  ?? null),
                        'fix_time'            => $toDate($row[5]  ?? null),
                        'fix_time_new_column' => $toDate($row[6]  ?? null),
                        'statement'           => $row[7]  ?? null,
                        'user_id'             => $measureId,
                        'gclid'               => $gclid,
                        'utm_content'         => $utmContent,
                        'device'              => $row[15] ?? null,
                        'referer'             => $row[14] ?? null,
                        'keyword'             => $row[11] ?? null,
                        'site_name'           => $row[12] ?? null,
                        'tclick'              => $tclickId,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('MP 計測データ取込完了');
        return Command::SUCCESS;
    }
}
