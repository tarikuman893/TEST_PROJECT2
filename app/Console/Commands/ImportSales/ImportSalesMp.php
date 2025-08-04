<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesMp extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:sales-mp';

    /** 説明 */
    protected $description = 'MP 売上 CSV を csv_mp へ取込む';

    /* ---------- ヘルパ ---------- */
    /**
     * 文字列日付 → Y-m-d H:i:s / null
     */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    /**
     * 税込金額文字列 → 税抜き整数（1円未満切り捨て）
     */
    private static function toIntTax(?string $v): int
    {
        $yen = (int) str_replace(',', '', $v ?? '0');
        return (int) floor($yen / 1.1);      // 10% 消費税を除去
    }

    public function handle(): int
    {
        /* ---------- 設定 ---------- */
        $sourceDir  = storage_path('app/csv/Sales');
        $csvPattern = "$sourceDir/*mp*.csv";;
        $headers    = ['タイプ'];                 // 見出し行判定用

        /* ---------- 取込 ---------- */
        foreach (glob($csvPattern) as $csv) {
            // SJIS → UTF-8 変換（BOM 付き CSV も安全に扱う）
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    // 空行／見出し行を除外
                    return empty($row) || in_array($row[0] ?? '', $headers, true);
                })
                ->each(function (array $row) {
                    DB::table('csv_mp')->insert([
                        'archive_type'        => $row[0] ?? null,                                                // タイプ
                        'pg_name'     => ($row[1] ?? '') . ($row[12] ?? ''),                              // プログラム名＋サイト名
                        'reward'      => self::toIntTax($row[2] ?? null),                                // 税抜き報酬
                        'click_time'  => self::toDate($row[3] ?? null),                                  // クリック日付
                        'occur_time'  => self::toDate($row[4] ?? null),                                  // 発生日
                        'fix_time'    => self::toDate($row[6] ?? null),                                  // 成果確定日
                        'statement'   => $row[7]  ?? null,                                               // ステータス
                        'user_id'     => $row[8]  ?? null,                                               // ユーザーID
                        'device'      => $row[9]  ?? null,                                               // 端末種類
                        'referer'     => $row[10] ?? null,                                               // リファラー(クリック)
                        'keyword'     => $row[11] ?? null,                                               // キーワード
                        'site_name'   => $row[12] ?? null,                                               // サイト名
                        'product_id'  => $row[13] ?? null,                                               // productID
                        'region'      => $row[14] ?? null,                                               // 地域
                        'af'          => $row[15] ?? null,                                               // af
                        // pageoptimizer (列 16) は不要のため未取込
                    ]);
                });

            unlink($tmp);
        }

        $this->info('MP 売上データ取込完了');
        return Command::SUCCESS;
    }
}
